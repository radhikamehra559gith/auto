# ========================================
# Firebase Video Automation v6 — GitHub Actions Ready
# ========================================

import os
import json
import shutil
import re
import uuid
import requests
import datetime
import subprocess
import sys
import firebase_admin
from firebase_admin import credentials, storage, firestore

# ========================================
# Firebase Setup
# ========================================
bot_id = "bot2"

firebase_credentials_json = os.getenv("FIREBASE_CREDENTIALS")

if not firebase_credentials_json:
    print("❌ Missing FIREBASE_CREDENTIALS environment variable!")
    sys.exit(1)

# Convert JSON string → dict
firebase_credentials_dict = json.loads(firebase_credentials_json)

bucket_name = "chat-app-13880.appspot.com"

if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_credentials_dict)
    firebase_admin.initialize_app(cred, {"storageBucket": bucket_name})

db = firestore.client()
bucket = storage.bucket()

print(f"🔥 Connected to Firestore collection: media")
print(f"☁️ Using bucket: {bucket_name}")

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
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            "-vf", f"scale={resolution}", "-c:v", "libx264", "-preset", "fast",
            "-c:a", "aac", output_file
        ]
        subprocess.run(cmd, check=True)
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
    blob.make_public()
    return blob.public_url.replace("/", "%2F").replace("%2F%3F", "/?")


# ========================================
# Logging / Verification
# ========================================
verify_db = firestore.client()

today_str = datetime.datetime.now().strftime("%Y-%m-%d")
log_collection = verify_db.collection(today_str)
bot_doc = log_collection.document(bot_id)
bot_snapshot = bot_doc.get()

if not bot_snapshot.exists:
    bot_doc.set({})
    bot_data = {}
else:
    bot_data = bot_snapshot.to_dict() or {}


def parse_runtime(rt):
    try:
        h, m, s = map(int, rt.replace("H", "").replace("M", "").replace("S", "").split("-"))
        return h + m/60 + s/3600
    except:
        return 0


total_runtime = sum(parse_runtime(v.get("active_time", "0H-0M-0S"))
                    for k, v in bot_data.items() if k.startswith("runtime_"))

# ✅ Hard stop (for GitHub Action)
if total_runtime >= 5:
    print(f"🛑 Total runtime for {bot_id} today is {total_runtime:.2f}h — limit reached. Stopping job.")
    sys.exit(0)

runtime_num = sum(1 for k in bot_data if k.startswith("runtime_")) + 1
runtime_key = f"runtime_{runtime_num}"

start_time = datetime.datetime.now()
bot_data[runtime_key] = {
    "started_at": start_time.isoformat(),
    "ended_at": "",
    "active_time": "",
    "status": "running",
    "logs": []
}
bot_doc.set(bot_data, merge=True)
print(f"🕒 Started {runtime_key}")

# ========================================
# Process unprocessed videos
# ========================================
collection_name = "media"
unprocessed_docs = list(db.collection(collection_name).where("processed", "==", False).stream())

if not unprocessed_docs:
    print("✅ No unprocessed videos found!")
    sys.exit(0)

for index, doc in enumerate(unprocessed_docs, start=1):
    print("=" * 50)
    print(f"🚀 Processing video {index}/{len(unprocessed_docs)}")
    print("=" * 50)

    data = doc.to_dict()
    video_url = data.get("url")
    if not video_url:
        print(f"⚠️ No URL in {doc.id}, skipping.")
        continue

    match = re.match(r"https://firebasestorage\.googleapis\.com/v0/b/([^/]+)/o/([^?]+)\?alt=media&token=(.+)", video_url)
    if not match:
        print(f"⚠️ Invalid Firebase URL in {doc.id}, skipping.")
        continue

    token = match.group(3)

    # Download
    video_filename = "input_video.mp4"
    with requests.get(video_url, stream=True) as r:
        r.raise_for_status()
        with open(video_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    duration = get_video_duration(video_filename)

    # Generate thumbnail
    thumbnail_file = "thumbnail.jpg"
    subprocess.run([
        "ffmpeg", "-y", "-i", video_filename, "-ss", str(duration/2),
        "-vframes", "1", thumbnail_file
    ])

    converted_files = create_quality_versions(video_filename)

    # Upload
    video_urls = {}
    for quality, path in converted_files.items():
        url = upload_to_firebase(path, token, quality)
        video_urls[quality] = url

    thumbnail_url = upload_to_firebase(thumbnail_file, token)

    # Update Firestore
    db.collection(collection_name).document(doc.id).update({
        "qualities": video_urls,
        "thumbnail": thumbnail_url,
        "duration": duration,
        "processed": True,
        "processedAt": datetime.datetime.now().isoformat()
    })

    shutil.rmtree("output_videos", ignore_errors=True)
    os.remove(video_filename)
    os.remove(thumbnail_file)

    bot_data[runtime_key]["logs"].append(doc.id)
    bot_doc.set(bot_data, merge=True)

# ========================================
# Update runtime info
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
bot_doc.set(bot_data, merge=True)
print(f"✅ Completed {runtime_key} | Active: {active_str} | Logs: {len(bot_data[runtime_key]['logs'])}")
