# Content:

* Hướng dẫn thiết lập môi trường conda,
* Mô tả chi tiết từng tầng Bronze / Silver / Gold,
* Giải thích chức năng từng container,
* Pipeline thực thi từ tải dữ liệu → upload → ETL → BI.

---

# 🚖 NYC Taxi Data Lakehouse — Batch Processing Pipeline

> **Mục tiêu:** Xây dựng hệ thống xử lý dữ liệu **batch-oriented data lakehouse** theo kiến trúc **Medallion (Bronze → Silver → Gold)**
> với các công nghệ: **Spark, Delta Lake, Hive Metastore, Trino, MinIO, và Metabase.**

---

## 🧩 Thành phần hệ thống

| Thành phần              | Chức năng                      | Công nghệ                         |
| ----------------------- | ------------------------------ | --------------------------------- |
| **Data Source**         | NYC Taxi Public Data (Parquet) | nyc.gov TLC datasets              |
| **Ingestion Scripts**   | Tải & upload dữ liệu           | Python (`requests`, `boto3`)      |
| **Storage Layer**       | Lưu trữ file dữ liệu           | MinIO (S3-compatible)             |
| **Processing Layer**    | ETL batch                      | Apache Spark + Delta Lake         |
| **Metadata Layer**      | Quản lý schema/table           | Hive Metastore (Postgres backend) |
| **Query Layer**         | Truy vấn dữ liệu bằng SQL      | Trino                             |
| **Visualization Layer** | Dashboard & BI                 | Metabase                          |

---

## 📁 Cấu trúc thư mục

```
DE-NYC/
├── data/
│   ├── raw/                # dữ liệu tải từ NYC
│   ├── bronze/             # output của Spark Bronze job
│   ├── silver/             # output của Spark Silver job
│   ├── gold/               # output của Spark Gold job
│   ├── download_nyc_data.py        # tải dữ liệu từ NYC Open Data
│   └── upload_data_to_Minio.py     # upload dữ liệu lên MinIO
│
├── spark/
│   ├── apps/
│   │   ├── bronze_nyc.py
│   │   ├── silver_trips.py
│   │   ├── gold_kpi.py
│   │   └── register_tables.py
│   ├── Dockerfile
│   └── conf/spark-defaults.conf
│
├── trino/
│   ├── catalog/
│   │   ├── hive.properties
│   │   └── delta.properties
│   └── etc/
│
├── spark-jupyter/
│   └── notebooks/
│
├── metabase-data/
│   └── metabase.db        # lưu metadata Metabase (SQLite)
│
├── Makefile
├── docker-compose.yaml
├── requirements.txt
├── .env
└── README.md
```

---

## ⚙️ Thiết lập môi trường làm việc

### 1️⃣ Tạo Conda environment

```bash
conda create -n de_env python=3.10 -y
conda activate de_env
```

### 2️⃣ Cài đặt các dependencies

```bash
pip install -r requirements.txt
```


## 🧰 Các script Python trong thư mục `data/`

### `download_nyc_data.py`

* Tải dữ liệu Parquet từ NYC Open Data API.
* Tự động tạo thư mục `/data/raw/nyc_taxi/` nếu chưa có.
* Cho phép tải nhiều dataset: `yellow`, `green`, `fhv`, `fhvhv`.

Ví dụ:

```bash
python data/download_nyc_data.py
```

### `upload_data_to_Minio.py`

* Upload dữ liệu từ `/data/raw/` lên MinIO bucket `datalake/raw/nyc_taxi/`..

```bash
python data/upload_data_to_Minio.py
```

---

## 🧱 Docker Compose — Môi trường Data Lakehouse

Các container được orchestrated bằng **Docker Compose**.

| Container            | Chức năng                             | Port           |
| -------------------- | ------------------------------------- | -------------- |
| **minio**            | Lưu trữ file S3                       | `9000`, `9001` |
| **metastore_db**     | PostgreSQL backend cho Hive Metastore | `5433`         |
| **hive-metastore**   | Service quản lý metadata (Thrift)     | `9083`         |
| **spark-master**     | Spark master node                     | `7077`, `8084` |
| **spark-worker-1/2** | Worker nodes thực thi job Spark       | —              |
| **trino**            | SQL engine query Delta & Hive         | `8080`         |
| **metabase**         | BI dashboard                          | `3000`         |

---

## 🧩 Makefile — Các lệnh điều khiển chính

```makefile
run-all:
	docker compose -f docker-compose.yaml up --build -d

down-rm:
	docker compose down -v

build-spark:
	docker build -t spark-nyc:latest ./spark

JOB ?=nyc_test.py
spark-submit:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 /opt/spark-apps/$(JOB)
```

| Lệnh                                  | Mô tả                           |
| ------------------------------------- | ------------------------------- |
| `make run-all`                        | Khởi động toàn bộ hệ thống      |
| `make down-rm`                        | Dừng container & xóa volumes    |
| `make build-spark`                    | Build Spark image với Delta JAR |
| `make spark-submit JOB=bronze_nyc.py` | Chạy Spark job cụ thể           |

---

## 🚀 Quy trình chạy toàn bộ pipeline

### 1️⃣ Khởi động hệ thống

```bash
make run-all
```

### 2️⃣ Tải dữ liệu NYC

```bash
python data/download_nyc_data.py
```

### 3️⃣ Upload lên MinIO

```bash
python data/upload_data_to_Minio.py
```

### 4️⃣ Chạy ETL jobs

#### Bronze Layer

```bash
make spark-submit JOB=bronze_nyc.py
```

* Đọc file từ `s3a://datalake/raw/nyc_taxi/`
* Thêm cột `year`, `month` từ tên file
* Chuẩn hóa schema
* Ghi Delta partitioned by (`year`, `month`) vào `bronze/`

#### Silver Layer

```bash
make spark-submit JOB=silver_trips.py
```

* Chuẩn hóa schema giữa các loại taxi (`yellow`, `green`, `fhv`, `fhvhv`)
* Làm sạch dữ liệu: loại bỏ null, lọc distance/time bất hợp lý
* Thêm cột: `duration_min`, `pickup_date`, `pickup_hour`
* Ghi Delta partitioned by (`pickup_date`, `service_type`) vào `silver/`

#### Gold Layer

```bash
make spark-submit JOB=gold_kpi.py
```

* Tổng hợp KPI:

  * **daily_revenue_by_zone**: doanh thu/ngày/khu vực
  * **hourly_demand_by_zone**: nhu cầu/giờ/khu vực
* Ghi Delta partitioned by (`pickup_date`, `service_type`) vào `gold/`

#### Register Tables

```bash
make spark-submit JOB=register_tables.py
```

* Tạo database `nyc_gold`
* Đăng ký các bảng Delta trong Hive Metastore.

---

## 📊 Kết nối & trực quan hóa với Metabase

### 1️⃣ Truy cập Metabase

```
http://localhost:3000
```

### 2️⃣ Thêm kết nối Trino

| Field            | Value        |
| ---------------- | ------------ |
| **Display name** | Trino        |
| **Host**         | `trino`      |
| **Port**         | `8080`       |
| **Catalog**      | `hive`       |
| **Schema**       | `nyc_gold`   |
| **Username**     | `trino`      |
| **Password**     | *(để trống)* |

### 3️⃣ Viết query trong Metabase

**+ New → SQL Query → Database: Trino**

```sql
SELECT pickup_date, service_type, SUM(revenue) AS total_revenue
FROM hive.nyc_gold.daily_revenue_by_zone
GROUP BY pickup_date, service_type
ORDER BY pickup_date
```

> ⚠️ Không dùng dấu `;` ở cuối câu.

### 4️⃣ Tạo dashboard

* Chọn **Visualization → Line chart / Bar chart**
* Save → **Add to dashboard** → “NYC Taxi Analytics”

---


## 🧩 Các truy vấn SQL tham khảo

```sql
-- Doanh thu theo ngày
SELECT pickup_date, service_type, SUM(revenue) AS total_revenue
FROM hive.nyc_gold.daily_revenue_by_zone
GROUP BY pickup_date, service_type
ORDER BY pickup_date;

-- Nhu cầu theo giờ
SELECT pickup_hour, service_type, SUM(trips) AS total_trips
FROM hive.nyc_gold.hourly_demand_by_zone
GROUP BY pickup_hour, service_type
ORDER BY pickup_hour;

-- Top 10 khu vực doanh thu cao nhất
SELECT pu_location_id, SUM(revenue) AS total_revenue
FROM hive.nyc_gold.daily_revenue_by_zone
GROUP BY pu_location_id
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## 🧹 Dọn dẹp hệ thống

```bash
make down-rm
```

> Xóa toàn bộ container & volume (bao gồm MinIO data, Postgres metadata, Metabase DB).

---
<!-- 
## 🔮 Định hướng mở rộng

| Mục tiêu                   | Hướng phát triển                                                   |
| -------------------------- | ------------------------------------------------------------------ |
| **Streaming Layer**        | PostgreSQL → Debezium → Kafka → Spark Structured Streaming → Delta |
| **Workflow Orchestration** | Airflow hoặc Dagster                                               |
| **Data Quality**           | Great Expectations cho Silver/Gold                                 |
| **Lineage & Governance**   | OpenMetadata / DataHub tích hợp Hive & Trino                       |
| **Monitoring**             | Prometheus + Grafana theo dõi Spark, Trino                         |

--- -->

## 🎯 Kết luận

Pipeline này cung cấp một nền tảng **Data Lakehouse hiện đại**, bao gồm:

* **ETL** với Spark + Delta Lake
* **Metadata** qua Hive Metastore
* **Query & BI** qua Trino + Metabase

→ Giúp xây dựng quy trình xử lý dữ liệu batch **chuẩn thực tế doanh nghiệp**, dễ mở rộng sang **real-time streaming** hoặc **machine learning pipeline**.

---
