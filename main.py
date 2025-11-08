# ========================================
# Firebase Video Automation v7 — GitHub Secrets + Limits
# ========================================

import os
import re
import uuid
import shutil
import json
import datetime
import subprocess
import requests
import firebase_admin
from firebase_admin import credentials, storage, firestore

# ========================================
# Environment Config
# ========================================
BOT_ID = os.getenv("BOT_ID", "bot2")
BUCKET_NAME = os.getenv("FIREBASE_BUCKET")
MAIN_FIREBASE_CREDENTIALS = os.getenv("FIREBASE_CREDENTIALS_MAIN")
VERIFY_FIREBASE_CREDENTIALS = os.getenv("FIREBASE_CREDENTIALS_VERIFY")

if not (MAIN_FIREBASE_CREDENTIALS and VERIFY_FIREBASE_CREDENTIALS and BUCKET_NAME):
    raise EnvironmentError("❌ Missing Firebase credentials or bucket name in environment variables.")

# ========================================
# Firebase Setup
# ========================================
# Load from secrets (JSON string)
main_creds = json.loads(MAIN_FIREBASE_CREDENTIALS)
verify_creds = json.loads(VERIFY_FIREBASE_CREDENTIALS)

if not firebase_admin._apps:
    main_app = firebase_admin.initialize_app(credentials.Certificate(main_creds), {"storageBucket": BUCKET_NAME})

if "verify_app" not in [app.name for app in firebase_admin._apps.values()]:
    verify_app = firebase_admin.initialize_app(credentials.Certificate(verify_creds), name="verify_app")

db = firestore.client()
bucket = storage.bucket()
verify_db = firestore.client(firebase_admin.get_app("verify_app"))

print(f"✅ Connected to Firebase bucket: {BUCKET_NAME}")

# ========================================
# Runtime Control
# ========================================
today = datetime.datetime.now()
today_str = today.strftime("%Y-%m-%d")
month_str = today.strftime("%Y-%m")

DAILY_LIMIT_MIN = 300       # 5 hours/day
MONTHLY_LIMIT_MIN = 2000    # 2000 minutes/month

# Daily log doc
log_doc = verify_db.collection(today_str).document(BOT_ID)
snapshot = log_doc.get()
bot_data = snapshot.to_dict() if snapshot.exists else {}

# Helper to parse time strings
def parse_runtime(rt):
    try:
        h, m, s = map(int, rt.replace("H", "").replace("M", "").replace("S", "").split("-"))
        return h * 60 + m + s / 60
    except:
        return 0

# Daily usage
daily_minutes = sum(parse_runtime(v.get("active_time", "0H-0M-0S"))
                    for k, v in bot_data.items() if k.startswith("runtime_"))
if daily_minutes >= DAILY_LIMIT_MIN:
    print(f"🛑 Daily limit reached ({daily_minutes:.1f} min). Exiting.")
    exit()

# Monthly usage
month_doc = verify_db.collection("monthly_logs").document(f"{BOT_ID}_{month_str}")
month_data = month_doc.get().to_dict() or {}
month_total = month_data.get("total_minutes", 0)
if month_total >= MONTHLY_LIMIT_MIN:
    print(f"🛑 Monthly limit reached ({month_total:.1f} min). Exiting.")
    exit()

# Start new runtime
runtime_key = f"runtime_{len(bot_data) + 1}"
start_time = datetime.datetime.now()
bot_data[runtime_key] = {
    "started_at": start_time.isoformat(),
    "ended_at": "",
    "active_time": "",
    "status": "running",
    "logs": []
}
log_doc.set(bot_data, merge=True)
print(f"🕒 Started {runtime_key}")

# ========================================
# Helper functions
# ========================================
def get_video_duration(filename):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    return float(result.stdout)

def create_quality_versions(input_file):
    qualities = {"360p": "640x360", "480p": "854x480", "720p": "1280x720"}
    output_files = {}
    os.makedirs("output_videos", exist_ok=True)
    for quality, resolution in qualities.items():
        output_file = f"output_videos/{uuid.uuid4()}-{quality}.mp4"
        subprocess.run([
            "ffmpeg", "-y", "-i", input_file,
            "-vf", f"scale={resolution}", "-c:v", "libx264",
            "-preset", "fast", "-c:a", "aac", output_file
        ], check=True)
        output_files[quality] = output_file
    return output_files

def upload_to_firebase(file_path, token, quality=None):
    filename = os.path.basename(file_path)
    if quality:
        blob_path = f"qualities/{quality}/{filename}"
    elif "thumbnail" in filename:
        blob_path = f"thumbnails/{filename}"
    else:
        blob_path = filename

    blob = bucket.blob(blob_path)
    blob.upload_from_filename(file_path)
    blob.metadata = {"firebaseStorageDownloadTokens": token}
    blob.patch()

    return (
        f"https://firebasestorage.googleapis.com/v0/b/{BUCKET_NAME}/o/"
        f"{blob_path.replace('/', '%2F')}?alt=media&token={token}"
    )

# ========================================
# Video Processing
# ========================================
collection_name = "media"
unprocessed_docs = list(db.collection(collection_name).where("processed", "==", False).stream())

print(f"🎯 Found {len(unprocessed_docs)} unprocessed videos")

for i, doc in enumerate(unprocessed_docs, start=1):
    data = doc.to_dict()
    video_url = data.get("url")
    if not video_url:
        continue

    match = re.match(r"https://firebasestorage\.googleapis\.com/v0/b/([^/]+)/o/([^?]+)\?alt=media&token=(.+)", video_url)
    if not match:
        continue

    token = match.group(3)
    video_filename = "input_video.mp4"

    # Download
    print(f"⬇️ Downloading {doc.id}")
    with requests.get(video_url, stream=True) as r:
        r.raise_for_status()
        with open(video_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    # Process
    duration = get_video_duration(video_filename)
    thumbnail_file = "thumbnail.jpg"
    subprocess.run(["ffmpeg", "-y", "-i", video_filename, "-ss", str(duration / 2),
                    "-vframes", "1", thumbnail_file])

    converted_files = create_quality_versions(video_filename)
    video_urls = {q: upload_to_firebase(p, token, q) for q, p in converted_files.items()}
    thumbnail_url = upload_to_firebase(thumbnail_file, token)

    # Update Firestore
    db.collection(collection_name).document(doc.id).update({
        "processed": True,
        "processedAt": datetime.datetime.now().isoformat(),
        "thumbnail": thumbnail_url,
        "qualities": video_urls,
        "duration": duration,
    })

    bot_data[runtime_key]["logs"].append(doc.id)
    log_doc.set(bot_data, merge=True)

    # Cleanup
    shutil.rmtree("output_videos", ignore_errors=True)
    os.remove(video_filename)
    os.remove(thumbnail_file)

# ========================================
# Update Logs
# ========================================
end_time = datetime.datetime.now()
elapsed = end_time - start_time
h, rem = divmod(elapsed.total_seconds(), 3600)
m, s = divmod(rem, 60)
active_str = f"{int(h)}H-{int(m)}M-{int(s)}S"

bot_data[runtime_key].update({
    "ended_at": end_time.isoformat(),
    "active_time": active_str,
    "status": "completed"
})
log_doc.set(bot_data, merge=True)

# Monthly total update
month_total += h * 60 + m + s / 60
month_doc.set({"total_minutes": month_total}, merge=True)

print(f"✅ Done! Active: {active_str} | Monthly total: {month_total:.1f} mins")
