# E-Commerce Data Pipeline and Analytics System
Hệ thống Data Pipeline và Phân tích Dữ liệu E-Commerce End-to-End xây dựng trên kiến trúc hiện đại kết hợp **Kafka, PostgreSQL, dbt, Apache Airflow và Streamlit**.

Dự án khai thác bộ dữ liệu **Olist Brazilian E-Commerce Dataset** nhằm thực hiện toàn bộ vòng đời dữ liệu: Ingestion & Streaming, Lưu trữ RAW Layer, Biến đổi dữ liệu (Transformation) với dbt, Orchestration với Airflow cho tới Trực quan hóa tương tác (Analytics & Visualization).

> **Lưu ý:** Apache Kafka được tích hợp để mô phỏng quá trình Real-time Data Streaming từ dữ liệu lịch sử (Historical Ingestion simulation).

---

## 1. Project Overview
Mục tiêu cốt lõi của dự án là xây dựng một pipeline xử lý dữ liệu chuẩn hóa đa tầng (Multi-layer Architecture) phục vụ nhu cầu phân tích kinh doanh E-Commerce.

### System Architecture
```text
       Olist E-Commerce Dataset (CSVs)
                      │
                      ▼
           Python Kafka Producer
                      │
                      ▼
                Apache Kafka
                      │
                      ▼
           Python Kafka Consumer
                      │
                      ▼
         PostgreSQL (RAW Layer Schema)
                      │
                      ▼
                 dbt Staging
                      │
                      ▼
    dbt Analytics Layer (Star Schema)
                      │
                      ▼
             Streamlit Dashboard
```

### Tech Stack
| Công nghệ | Vai trò trong Hệ thống |
|---|---|
| **Python 3.11** | Viết script Ingestion, Kafka Producer/Consumer |
| **Apache Kafka & Zookeeper** | Event Streaming Platform (Message Broker) |
| **PostgreSQL** | Relational Database lưu trữ RAW Layer & Analytics Layer |
| **dbt (data build tool)** | Data Transformation, Data Modeling & Data Quality Testing |
| **Apache Airflow** | Workflow Management & Pipeline Orchestration |
| **Streamlit & Plotly** | Interactive Analytics Dashboard & Visualization |
| **Docker & Docker Compose** | Containerization toàn bộ hạ tầng dịch vụ |

## 2. Data Pipeline Architecture
Dự án xử lý bộ dữ liệu thương mại điện tử Olist với **9 bảng dữ liệu chính**:

| Dataset | Số lượng bản ghi |
|---|---:|
| `Customers` | 99,441 |
| `Orders` | 99,441 |
| `Order Items` | 112,650 |
| `Payments` | 103,886 |
| `Reviews` | 99,224 |
| `Products` | 32,951 |
| `Sellers` | 3,095 |
| `Geolocation` | 1,000,163 |
| `Category Translation` | 71 |

### Data Layers
1. **RAW Layer (`raw` schema):** Dữ liệu được Kafka Consumer nạp vào PostgreSQL để lưu trữ dữ liệu nguồn trước khi transformation.

2. **Staging Layer (`staging` schema):** dbt thực hiện chuẩn hóa kiểu dữ liệu, làm sạch timestamp, xử lý dữ liệu và deduplication phù hợp.

3. **Analytics Layer (`analytics` schema):** Tổ chức dữ liệu theo mô hình **Star Schema** phục vụ các truy vấn phân tích (OLAP).
### Star Schema Model
**Dimension Tables**
- `dim_customers`
- `dim_products`
- `dim_sellers`

**Fact Tables**
- `fact_orders`
- `fact_order_items`
- `fact_payments`
- `fact_reviews`
Các bảng được liên kết thông qua các khóa tương ứng để phục vụ phân tích đơn hàng, khách hàng, sản phẩm, thanh toán và đánh giá.

## 3. Orchestration & Data Quality
### Workflow Orchestration
Pipeline được điều phối thông qua Apache Airflow với DAG:
```text
[Kafka Producer]
       │
       ▼
[Kafka Consumer]
       │
       ▼
[dbt Run Models]
       │
       ▼
[dbt Test Quality]
```
Airflow chịu trách nhiệm điều phối thứ tự thực hiện các bước trong pipeline.

### Data Quality Framework
Sử dụng tính năng Testing của **dbt** để kiểm tra chất lượng và tính toàn vẹn của dữ liệu:
- **`unique`**: Kiểm tra tính duy nhất của khóa.
- **`not_null`**: Kiểm tra các trường bắt buộc không bị NULL.
- **`relationships`**: Kiểm tra mối quan hệ giữa các bảng.
- **Custom Data Tests**: Kiểm tra một số logic nghiệp vụ và tính hợp lệ của dữ liệu.

## 4. Project Structure
```text
Project_CV/
├── airflow/
│   └── dags/                  # Airflow DAGs định nghĩa Workflow Pipeline
├── data/
│   └── raw/                   # Thư mục lưu trữ Olist CSV Datasets
├── dbt/
│   └── ecommerce_analytics/   # dbt Project (Models, Macros, Tests, Schema)
├── scripts/
│   ├── ingest_olist_csv.py    # Script nạp dữ liệu thô
│   ├── kafka_consumer.py      # Kafka Consumer ghi dữ liệu vào PostgreSQL
│   ├── kafka_producer.py      # Kafka Producer đẩy sự kiện từ CSV
│   └── setup_dbt_profile.sh   # Bash script cấu hình profiles.yml cho dbt
├── streamlit/
│   └── dashboard.py           # Mã nguồn giao diện Streamlit Dashboard
├── venv/                      # Python Virtual Environment (Local)
├── .gitignore                 # Cấu hình bỏ qua các file tạm/log khi Commit
├── docker-compose.yml         # Container Orchestration (Postgres, Kafka, Airflow)
├── Dockerfile                 # Custom Docker Image setup
├── init_dbt.sh                # Script khởi tạo dbt môi trường container
├── pyrightconfig.json         # Cấu hình Pylance / Type Checker
├── README.md                  # Tài liệu hướng dẫn dự án
├── requirements.txt           # Danh sách các thư viện Python phụ thuộc
└── settings.json              # Cấu hình Workspace VS Code
```

## 5. Installation & Usage
### Prerequisites
- Docker Desktop
- Docker Compose
- Python 3.11+
- Git

### Clone Repository
```bash
git clone <your-repository-url>
cd Project_CV
```

### Start Services
Khởi chạy toàn bộ infrastructure:
```bash
docker compose up -d
```

Kiểm tra trạng thái các service:
```bash
docker compose ps
```
Các service chính gồm:
- PostgreSQL
- Kafka
- Zookeeper
- Airflow

### Run Streamlit Dashboard
Nếu chạy dashboard trực tiếp trên máy local:
```bash
streamlit run streamlit/dashboard.py
```
Dashboard được cung cấp tại:
```text
http://localhost:8501
```

### Pipeline Flow
```text
Olist CSV
   ↓
Python Producer
   ↓
Kafka
   ↓
Python Consumer
   ↓
PostgreSQL RAW
   ↓
dbt Staging
   ↓
dbt Analytics
   ↓
Streamlit Dashboard
```

## 6. Key Analytics Capabilities
Dashboard cung cấp các nhóm phân tích chính:
### Sales Analysis
- Total Revenue
- Total Orders
- Average Order Value (AOV)
- Revenue Trend
- Revenue by State

### Product Analysis
- Revenue by Product Category
- Order Items
- Product Performance

### Customer Analysis
- Customer Distribution
- Customer Location
- Orders by State

### Payment & Review Analysis
- Payment Type Distribution
- Review Score Distribution
- Order Status Distribution

### Logistics & Delivery
- Delivery Performance
- Delivery Delay Analysis
- Delivery Trend

## 7. Results
Project đã hoàn thiện các thành phần chính của một Data Pipeline E-Commerce:
- Python Data Ingestion
- Kafka Producer/Consumer
- PostgreSQL RAW Layer
- dbt Staging Layer
- dbt Analytics Layer
- Star Schema
- Data Quality Testing
- Apache Airflow DAG
- Streamlit Analytics Dashboard
- Docker-based Environment

Một số bảng Analytics chính đã được kiểm tra:
```text
fact_orders
→ 99,441 orders

fact_order_items
→ 112,650 order items
```
Dữ liệu sau Transformation được sử dụng trực tiếp cho việc phân tích và trực quan hóa trên Streamlit Dashboard.

## 8. Future Improvements
- [ ] Triển khai xử lý dữ liệu tăng tiến (**dbt Incremental Models**).
- [ ] Bổ sung cơ chế **Kafka Dead Letter Queue (DLQ)** cho message lỗi.
- [ ] Tích hợp hệ thống monitoring cho Pipeline.
- [ ] Triển khai hệ thống lên nền tảng Cloud như AWS hoặc GCP.

## Author
**Thịnh Vũ**

