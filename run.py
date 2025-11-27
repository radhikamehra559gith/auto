import os
import requests
import logging
from supabase import create_client, Client

# -----------------------
# Setup logging
# -----------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -----------------------
# Load environment variables
# -----------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_TOKEN = os.environ.get("API_TOKEN")
ZONE_NAME = os.environ.get("ZONE_NAME")

if not all([SUPABASE_URL, SUPABASE_KEY, API_TOKEN, ZONE_NAME]):
    logging.error("Missing one or more required environment variables.")
    exit(1)

# -----------------------
# Initialize Supabase client
# -----------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# Setup Cloudflare headers
# -----------------------
headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

# -----------------------
# Step 1: Get Cloudflare Zone ID
# -----------------------
try:
    zone_resp = requests.get(f"https://api.cloudflare.com/client/v4/zones?name={ZONE_NAME}", headers=headers).json()
    ZONE_ID = zone_resp['result'][0]['id']
except Exception as e:
    logging.error(f"Failed to fetch Cloudflare zone ID: {e}")
    exit(1)

# -----------------------
# Step 2: Fetch unprocessed DNS records from Supabase
# -----------------------
try:
    response = supabase.table("records").select("*").eq("processed", False).execute()
    records = response.data
except Exception as e:
    logging.error(f"Failed to fetch records from Supabase: {e}")
    exit(1)

if not records:
    logging.info("No unprocessed records.")
    exit(0)
else:
    logging.info(f"Found {len(records)} unprocessed records.")

# -----------------------
# Step 3: Process each record
# -----------------------
for record in records:
    perform = record.get("perform", "create").lower()
    record_id = record.get("id")  # Supabase record ID

    try:
        if perform == "create":
            data = {
                "type": record["type"],
                "name": record["name"],
                "content": record["content"],
                "ttl": record.get("ttl", 3600),
                "proxied": record.get("proxied", False)
            }
            resp = requests.post(f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records",
                                 headers=headers, json=data)

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
                    "proxied": record.get("proxied", False)
                }
                resp = requests.put(
                    f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records/{cf_id}",
                    headers=headers, json=data
                )
            else:
                logging.warning(f"Record {record['name']} not found in Cloudflare to update.")
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
                logging.warning(f"Record {record['name']} not found in Cloudflare to delete.")
                continue

        else:
            logging.warning(f"Unknown perform action: {perform} for record {record['name']}")
            continue

        # -----------------------
        # Step 4: Update Supabase if successful
        # -----------------------
        success = resp.json().get("success", False) if perform != "delete" else resp.status_code == 200
        if success:
            supabase.table("records").update({"processed": True}).eq("id", record_id).execute()
            logging.info(f"{perform.capitalize()} successful for {record['name']}")
        else:
            logging.error(f"{perform.capitalize()} failed for {record['name']}: {resp.text}")

    except Exception as e:
        logging.error(f"Error processing record {record['name']}: {e}")
