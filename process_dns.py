# scripts/process_dns.py

from supabase import create_client
import requests
import json
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

# --- Step 1: Get Cloudflare Zone ID ---
zone_resp = requests.get(
    f"https://api.cloudflare.com/client/v4/zones?name={ZONE_NAME}",
    headers=headers
).json()

ZONE_ID = zone_resp['result'][0]['id']

# --- Step 2: Fetch unprocessed DNS records from Supabase ---
response = supabase.table("records").select("*").eq("processed", False).execute()
records = response.data

if not records:
    print("No unprocessed records.")
    exit()

print(f"Found {len(records)} unprocessed records.")

# --- Step 3: Process each record ---
for record in records:
    perform = record.get("perform", "create").lower()
    record_id = record.get("id")

    try:
        if perform == "create":
            data = {
                "type": record["type"],
                "name": record["name"],
                "content": record["content"],
                "ttl": record.get("ttl", 3600),
                "proxied": record.get("proxied", False),
            }
            resp = requests.post(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records",
                headers=headers, json=data
            )

        elif perform == "update":
            cf_resp = requests.get(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records?name={record['name']}",
                headers=headers
            ).json()

            if cf_resp['result']:
                cf_id = cf_resp['result'][0]['id']
                data = {
                    "type": record["type"],
                    "name": record["name"],
                    "content": record["content"],
                    "ttl": record.get("ttl", 3600),
                    "proxied": record.get("proxied", False),
                }
                resp = requests.put(
                    f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records/{cf_id}",
                    headers=headers, json=data
                )
            else:
                print(f"Record {record['name']} not found in Cloudflare for update.")
                continue

        elif perform == "delete":
            cf_resp = requests.get(
                f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records?name={record['name']}",
                headers=headers
            ).json()

            if cf_resp['result']:
                cf_id = cf_resp['result'][0]['id']
                resp = requests.delete(
                    f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records/{cf_id}",
                    headers=headers
                )
            else:
                print(f"Record {record['name']} not found in Cloudflare to delete.")
                continue

        else:
            print(f"Unknown perform action: {perform}")
            continue

        success = resp.json().get("success", False) if perform != "delete" else resp.status_code == 200

        if success:
            supabase.table("records").update({"processed": True}).eq("id", record_id).execute()
            print(f"{perform.capitalize()} successful for {record['name']}")
        else:
            print(f"{perform.capitalize()} failed for {record['name']}: {resp.text}")

    except Exception as e:
        print(f"Error processing record {record['name']}: {e}")
