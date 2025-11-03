import os
import requests
from datetime import datetime
from upload_data_to_Minio import MinioClient

# ---------------------------
# ⚙️ CONFIG
# ---------------------------
DATASETS = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "fhv": "fhv_tripdata",
    "fhvhv": "fhvhv_tripdata"
}

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LOCAL_DIR = "./data/raw/nyc_raw"
BUCKET = "datalake"
PREFIX = "raw/nyc_taxi"

# ---------------------------
# 📦 INIT MINIO
# ---------------------------
minio_client = MinioClient(stage='local')
minio_client.ensure_bucket(BUCKET)

# ---------------------------
# 📥 HÀM TẢI FILE
# ---------------------------
def download_file(url, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        print(f"⬇️  Downloading: {url}")
        r = requests.get(url, stream=True, timeout=30)
        if r.status_code == 200:
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Saved: {local_path}")
            return True
        else:
            print(f"⚠️ Skipped {url} (status={r.status_code})")
            return False
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        return False

# ---------------------------
# 🚀 TẢI DỮ LIỆU NYC
# ---------------------------
def fetch_nyc_data(year=2024, months=[6, 7, 8]):
    for dataset, prefix_name in DATASETS.items():
        for month in months:
            file_name = f"{prefix_name}_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}/{file_name}"
            local_path = f"{LOCAL_DIR}/{dataset}/{file_name}"
            download_file(url, local_path)
            # if download_file(url, local_path):
            #     # Upload lên MinIO
            #     object_name = f"{PREFIX}/{dataset}/{file_name}"
            #     minio_client.upload_file(BUCKET, object_name, local_path)

if __name__ == "__main__":
    # 🔹 Bạn có thể thay đổi tháng/năm ở đây
    fetch_nyc_data(year=2024, months=[6, 7, 8])
