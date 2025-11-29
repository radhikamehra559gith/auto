from supabase import create_client
import requests
import os

# --- Supabase config ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Cloudflare config ---
API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
ZONE_NAME = os.getenv("CLOUDFLARE_ZONE_NAME")

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# --- Get Cloudflare Zone ID ---
zone_resp = requests.get(
    f"https://api.cloudflare.com/client/v4/zones?name={ZONE_NAME}",
    headers=headers
).json()

ZONE_ID = zone_resp["result"][0]["id"]

# --- Fetch unprocessed records from Supabase ---
response = supabase.table("records").select("*").eq("processed", False).execute()
records = response.data or []

if not records:
    print("No unprocessed records.")
    exit(0)

print(f"Found {len(records)} unprocessed records.")

# --- Helpers ---
def parse_mx_content(content):
    """Convert '10 mail.example.com' -> priority=10, content='mail.example.com'"""
    try:
        parts = content.split(" ", 1)
        return int(parts[0]), parts[1]
    except Exception as e:
        raise ValueError(f"Invalid MX format: '{content}'") from e

def fix_proxied(record_type, proxied):
    """Force proxied=False for record types not allowed"""
    if record_type in ["MX", "TXT", "NS"]:
        return False
    return proxied

def full_name_for_cf(name):
    """Return FQDN for update/delete: append zone name if not already included"""
    if name.endswith(ZONE_NAME):
        return name
    return f"{name}.{ZONE_NAME}"

# --- Process records ---
for record in records:
    perform = record.get("perform", "create").lower()
    record_id = record["id"]
    rtype = record["type"].upper()
    proxied = fix_proxied(rtype, record.get("proxied", False))

    # Base payload
    data = {
        "type": rtype,
        "name": record["name"],  # keep as-is for create
        "ttl": record.get("ttl", 3600),
        "proxied": proxied
    }

    # Handle record-specific content
    if rtype == "MX":
        priority, mx_host = parse_mx_content(record["content"])
        data["priority"] = priority
        data["content"] = mx_host
    elif rtype == "TXT":
        content = record["content"]
        if not (content.startswith('"') and content.endswith('"')):
            content = f'"{content}"'
        data["content"] = content
    else:
        data["content"] = record["content"]

    try:
        # --- CREATE ---
        if perform == "create":
            resp = requests.post(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records",
                headers=headers, json=data
            )

            # Retry A/CNAME without proxy if blocked
            if not resp.json().get("success") and proxied:
                print(f"Retrying {rtype} without proxied for {record['name']}...")
                data["proxied"] = False
                resp = requests.post(
                    f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records",
                    headers=headers, json=data
                )

        # --- UPDATE ---
        elif perform == "update":
            record_name = full_name_for_cf(record['name'])
            cf = requests.get(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records?name={record_name}",
                headers=headers
            ).json()

            if not cf["result"]:
                print(f"No existing record to update: {record_name}")
                continue

            cf_id = cf["result"][0]["id"]
            resp = requests.put(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records/{cf_id}",
                headers=headers,
                json={**data, "name": record_name}  # use full name for update
            )

        # --- DELETE ---
        elif perform == "delete":
            record_name = full_name_for_cf(record['name'])
            cf = requests.get(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records?name={record_name}",
                headers=headers
            ).json()

            if not cf["result"]:
                print(f"No record found to delete: {record_name}")
                continue

            cf_id = cf["result"][0]["id"]
            resp = requests.delete(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records/{cf_id}",
                headers=headers
            )

        else:
            print(f"Unknown perform action: {perform}")
            continue

        # --- Mark as processed ---
        success = resp.status_code == 200 and resp.json().get("success", True)
        if success:
            supabase.table("records").update({"processed": True}).eq("id", record_id).execute()
            print(f"{perform.upper()} OK → {record['name']}")
        else:
            print(f"{perform.upper()} FAILED → {record['name']}: {resp.text}")

    except Exception as e:
        print(f"Error processing {record['name']}: {e}")
