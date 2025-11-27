import os
import time
import requests
from supabase import create_client
import logging

# -------------------
# Setup logging
# -------------------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# -------------------
# Load secrets from environment
# -------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
CLOUDFLARE_TOKEN = os.environ.get("CLOUDFLARE_TOKEN")
ZONE_NAME = os.environ.get("ZONE_NAME")

# -------------------
# Initialize clients
# -------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {
    "Authorization": f"Bearer {CLOUDFLARE_TOKEN}",
    "Content-Type": "application/json"
}

# Get Cloudflare Zone ID once
zone_resp = requests.get(f"https://api.cloudflare.com/client/v4/zones?name={ZONE_NAME}", headers=headers).json()
ZONE_ID = zone_resp['result'][0]['id']
logging.info(f"Cloudflare Zone ID: {ZONE_ID}")

# -------------------
# DNS Sync function
# -------------------
def sync_dns():
    response = supabase.table("records").select("*").eq("processed", False).execute()
    records = response.data

    if not records:
        logging.info("No unprocessed records.")
        return

    logging.info(f"Found {len(records)} unprocessed records.")

    for record in records:
        perform = record.get("perform", "create").lower()
        record_id = record.get("id")

        try:
            # --- CREATE ---
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

            # --- UPDATE ---
            elif perform == "update":
                cf_resp = requests.get(f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records?name={record['name']}",
                                       headers=headers).json()
                if cf_resp['result']:
                    cf_id = cf_resp['result'][0]['id']
                    data = {
                        "type": record["type"],
                        "name": record["name"],
                        "content": record["content"],
                        "ttl": record.get("ttl", 3600),
                        "proxied": record.get("proxied", False)
                    }
                    resp = requests.put(f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records/{cf_id}",
                                        headers=headers, json=data)
                else:
                    logging.warning(f"Record {record['name']} not found in Cloudflare for update.")
                    continue

            # --- DELETE ---
            elif perform == "delete":
                cf_resp = requests.get(f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records?name={record['name']}",
                                       headers=headers).json()
                if cf_resp['result']:
                    cf_id = cf_resp['result'][0]['id']
                    resp = requests.delete(f"https://api.cloudflare.com/client/v4/zones/{ZONE_ID}/dns_records/{cf_id}",
                                           headers=headers)
                else:
                    logging.warning(f"Record {record['name']} not found in Cloudflare for delete.")
                    continue

            else:
                logging.warning(f"Unknown perform action: {perform} for {record['name']}")
                continue

            # --- Mark processed if successful ---
            success = resp.json().get("success", False) if perform != "delete" else resp.status_code == 200
            if success:
                supabase.table("records").update({"processed": True}).eq("id", record_id).execute()
                logging.info(f"{perform.capitalize()} successful for {record['name']}")
            else:
                logging.error(f"{perform.capitalize()} failed for {record['name']}: {resp.text}")

        except Exception as e:
            logging.error(f"Error processing record {record['name']}: {e}")


# -------------------
# Main loop
# -------------------
if __name__ == "__main__":
    logging.info("Starting DNS sync worker...")
    while True:
        sync_dns()
        time.sleep(300)  # wait 5 minutes before next run
