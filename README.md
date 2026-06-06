# 🛒 Olist Brazilian E-Commerce — Modern Data Stack

![Status](https://img.shields.io/badge/Status-Production-green)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9-red)
![Tests](https://img.shields.io/badge/Tests-37%2F37%20Passing-brightgreen)

---

## 📋 Business Problem

Olist, a Brazilian e-commerce marketplace, generates massive volumes of transactional data across orders, customers, products, sellers, payments, and reviews. This raw data sits in disconnected CSV files with no unified analytical layer, making it impossible for business teams to answer critical questions about revenue trends, delivery performance, customer behaviour, and seller efficiency in real time.

---

## 🏗️ Architecture

```
Kaggle CSVs → Python → Snowflake RAW (Bronze)
                              ↓ dbt staging
                       Snowflake STAGING (Silver)
                              ↓ dbt marts
                        Snowflake MARTS (Gold)
                              ↓ Power BI
                         4 Dashboard Pages
     ↑ Orchestrated by Apache Airflow (Docker) ↑
```

### Medallion Architecture — Three Layers

| Layer | Schema | Purpose | Materialization |
|-------|--------|---------|----------------|
| 🥉 Bronze | `olist_db.raw` | Exact copy of source CSV files. No transformation. Append-only. Load timestamp added. | Tables (permanent) |
| 🥈 Silver | `olist_db.staging` | Cleaned, renamed, typed, deduplicated. Business rules applied. Tests enforced. | Views (lightweight) |
| 🥇 Gold | `olist_db.marts` | Star schema — Fact and Dimension tables. Business-ready. Power BI source. | Tables + Incremental |

---

## 🛠️ Tech Stack

| Tool | Purpose | Version |
|------|---------|---------|
| Snowflake | Cloud Data Warehouse | Latest |
| dbt Core | SQL Transformations | 1.8.0 |
| Apache Airflow | Pipeline Orchestration | 2.9.1 |
| Docker & Docker Compose | Containerisation | Latest |
| Power BI Desktop | Analytics Dashboards | Desktop |
| Python | Data Ingestion | 3.11 |
| dbt-utils | dbt Package | 1.x |
| dbt-expectations | dbt Package | 0.10.x |

---

## 📊 Dataset

**Source:** Kaggle — Olist Brazilian E-Commerce Public Dataset

| CSV File | Rows | Description |
|----------|------|-------------|
| `olist_orders_dataset.csv` | 99,441 | Core order records — status, timestamps, customer link |
| `olist_customers_dataset.csv` | 99,441 | Customer IDs, unique IDs, city, state, zip code |
| `olist_products_dataset.csv` | 32,951 | Product catalog — category, weight, dimensions |
| `olist_sellers_dataset.csv` | 3,095 | Seller profiles — location details |
| `olist_order_items_dataset.csv` | 112,650 | Order line items — product, seller, price, freight |
| `olist_order_payments_dataset.csv` | 103,886 | Payment records — type, installments, value |
| `olist_order_reviews_dataset.csv` | 99,224 | Customer reviews — score, comments, dates |
| `olist_geolocation_dataset.csv` | 1,000,163 | Zip code to latitude/longitude mapping |
| `product_category_translation.csv` | 71 | Portuguese to English category name translations |

---

## 🚀 Quick Start

```bash
cd olist-data-engineering
cp .env.example .env  # fill in Snowflake credentials
docker-compose up -d  # starts Airflow at localhost:8080
```

---

## 📁 Project Folder Structure

```
olist-data-engineering/
│
├── .env                          
├── .gitignore                   
├── docker-compose.yml            
├── README.md                    
│
├── airflow/
│   ├── Dockerfile                
│   ├── requirements.txt          
│   ├── dags/
│   │   ├── olist_pipeline_dag.py        
│   │   └── olist_data_quality_dag.py     
│   ├── logs/                     
│   └── plugins/                 
│
├── olist_dbt/
│   ├── dbt_project.yml           
│   ├── packages.yml              
│   ├── profiles.yml              
│   ├── models/
│   │   ├── staging/              
│   │   ├── intermediate/         
│   │   └── marts/core/          
│   ├── macros/                   
│   ├── snapshots/                
│   ├── seeds/                    
│   └── tests/                    
│
├── ingestion/
│   └── upload_to_snowflake.py    
│
├── data/                                   
```

---

## 🔧 Data Pipeline — Step by Step

| # | Task | What Happens | Output |
|---|------|--------------|--------|
| 1 | Kaggle Download | 9 CSV files downloaded from Kaggle dataset. Placed in local `/data` folder. | 9 CSV files in `/data/` |
| 2 | Python Ingestion | `upload_to_snowflake.py` reads each CSV with pandas, uppercases columns, adds `_LOAD_TIMESTAMP`, truncates target table, calls `write_pandas` to bulk load. | 9 raw tables in `olist_db.raw` |
| 3 | dbt deps + seed | Installs dbt packages (dbt-utils, dbt-expectations). Loads `product_category_translation.csv` seed file into raw schema. | `dbt_packages/`, `raw.product_category_translation` |
| 4 | dbt snapshot | Runs `customers_snapshot.sql`. Detects changes in `customer_city`, `customer_state`, `zip_code`. Inserts new SCD2 row with `valid_from`/`valid_to` timestamps. | `olist_db.snapshots.customers_snapshot` |
| 5 | dbt run staging | Builds 8 staging views: clean column names, cast data types, deduplicate rows, derive business metrics (`delivery_delay_days`, `review_sentiment`, `item_total`). | 8 views in `olist_db.staging` |
| 6 | dbt run marts | Builds 2 ephemeral intermediate models. Then builds 4 fact tables (2 incremental), 4 dimension tables, 1 aggregated mart. | 9 tables in `olist_db.marts` |
| 7 | dbt test | Runs 37 tests: unique, not_null, accepted_values, dbt_expectations range checks, singular business rule tests (`revenue_positive`, `no_future_orders`). | 37/37 PASS — green pipeline |
| 8 | dbt docs generate | Generates full lineage graph, column descriptions, test results. Browsable at `localhost:8080/dbt-docs`. | `target/catalog.json`, `manifest.json` |
| 9 | Power BI Refresh | Power BI connects to Snowflake Gold layer via DirectQuery. 4 dashboard pages auto-refresh with latest mart data. | 4 interactive dashboard pages |

---

## 🗄️ Data Model (Gold Layer Star Schema)

### Fact Tables

| Fact Table | Grain | Type | Key Columns & Business Purpose |
|------------|-------|------|-------------------------------|
| `fct_orders` | Per order | Incremental | Central fact: revenue, delivery metrics, review score, `basket_size_bucket`, `revenue_tier`. 34 columns. |
| `fct_order_items` | Per order×item | Incremental | Line-item fact: price, freight, `item_total`, `freight_pct_of_price`, `freight_tier`. FK to `dim_products`, `dim_sellers`. |

### Dimension Tables

| Dimension | Type | Key Attributes |
|-----------|------|----------------|
| `dim_customers` | SCD Type 2 | `customer_sk` (surrogate), `customer_id`, `customer_unique_id`, city, state, lat/lon, `valid_from`, `valid_to`, `is_current` |
| `dim_products` | Static | `product_id`, `category_en`, `category_pt`, `weight_g`, `volume_cm3`, `weight_tier`, `photo_tier` |
| `dim_sellers` | Enriched | `seller_id`, city, state, lat/lon + performance: `total_gmv`, `late_delivery_rate`, `avg_delivery_days`, `seller_tier` |
| `dim_date` | Date Spine | `date_id`, year, quarter, month, week, `day_name`, `is_weekend`, `is_weekday`, `is_public_holiday_br`, `days_ago`, `months_ago` |

### Star Schema Diagram

```
                    DIM_DATE
                       | (1)
                       * DATE_FK
DIM_CUSTOMERS ─(1)─── FCT_ORDERS ────(1)── DIM_DATE
                           | (1)
                           * ORDER_ID
                     FCT_ORDER_ITEMS
                      *            *
                PRODUCT_ID      SELLER_ID
                   | (1)           | (1)
              DIM_PRODUCTS      DIM_SELLERS ─(1)─ AGG_SELLER_PERFORMANCE
```

### Power BI Relationship Model

| # | From Table | Column | To Table | Column | Cardinality | Active |
|---|------------|--------|----------|--------|-------------|--------|
| 1 | `FCT_ORDERS` | `DATE_FK` | `DIM_DATE` | `DATE_ID` | <>:1 | ✅ Yes |
| 2 | `FCT_ORDERS` | `CUSTOMER_ID` | `DIM_CUSTOMERS` | `CUSTOMER_ID` | <>:1 | ✅ Yes |
| 3 | `FCT_ORDER_ITEMS` | `ORDER_ID` | `FCT_ORDERS` | `ORDER_ID` | <>:1 | ✅ Yes |
| 4 | `FCT_ORDER_ITEMS` | `PRODUCT_ID` | `DIM_PRODUCTS` | `PRODUCT_ID` | <>:1 | ✅ Yes |
| 5 | `FCT_ORDER_ITEMS` | `SELLER_ID` | `DIM_SELLERS` | `SELLER_ID` | <>:1 | ✅ Yes |
| 6 | `AGG_SELLER_PERFORMANCE` | `SELLER_ID` | `DIM_SELLERS` | `SELLER_ID` | <>:1 | ✅ Yes |

---

## 🧪 Data Quality, Observability & Monitoring

### 1. Robust dbt Integrity Validations

To prevent structural contamination or duplicate processing runs within the analytical Marts layer, the framework runs multi-tier data quality validation checks directly within the dbt compilation flow:

* **Generic Testing Suites:** Automated verification models enforcing `unique` and `not_null` validation rules on primary keys (such as `order_id`, `product_key`, and `customer_key`).
* **Relational Referential Integrity:** Upstream `relationships` validations that confirm all child keys inside fact tables find a structural match inside corresponding dimension tables before code deployment finishes.
* **Singular Business-Rule Testing:** Custom, hand-coded SQL testing assertions checking for impossible operational anomalies (e.g., verifying that data rows are flagged if an order's actual delivery timestamp predates its initial purchase timestamp).

### 2. Failure-Aware Airflow Alerting System

Data pipelines must alert engineers instantly when issues arise. The Airflow orchestrator features dedicated custom monitoring mechanisms:

* **Slack/Discord Webhook Plugins:** Programmatic scripts connected to execution failure hooks (`on_failure_callback`).
* **Real-time Diagnostic Pushes:** If any processing stage fails within Snowflake or dbt, the DAG instantly pushes diagnostic parameters (such as the specific task ID, active execution date, and direct log URLs) straight to engineering communication channels for immediate troubleshooting.

---

## 🎛️ Airflow DAG Documentation

### DAG Task Flow

| Task | Operator | Description |
|------|----------|-------------|
| `start` | EmptyOperator | Pipeline entry marker |
| `ingest_csvs_to_snowflake` | PythonOperator | Calls `upload_to_snowflake.main()` — loads all 9 CSVs to raw schema |
| `dbt_deps` | BashOperator | `dbt deps` — installs dbt-utils, dbt-expectations, audit-helper |
| `dbt_seed` | BashOperator | `dbt seed --full-refresh` — loads translation CSV |
| `dbt_snapshot` | BashOperator | `dbt snapshot` — runs SCD Type 2 on customers |
| `dbt_run_staging` | BashOperator | `dbt run --select staging` — builds 8 silver views |
| `dbt_run_marts` | BashOperator | `dbt run --select intermediate marts` — builds all gold tables |
| `dbt_test` | BashOperator | `dbt test` — runs all 37 tests across all layers |
| `dbt_docs_generate` | BashOperator | `dbt docs generate` — updates lineage + docs site |
| `end` | EmptyOperator | Pipeline exit marker (`trigger_rule: ALL_DONE`) |

---

## 📊 Power BI Dashboard Guide

### Dashboard Pages

| Page | Purpose | Key Visuals | Key KPIs |
|------|---------|-------------|----------|
| 📈 Sales Overview | Executive revenue summary | GMV trend, order status donut, payment type bar | GMV R$16.01M, AOV R$161, 99,441 orders |
| 👥 Customer Insights | Customer behaviour & location | Customers per month line, city orders bar, state map | 99,441 customers, 4.09 review score |
| 📦 Product Performance | Category & product analysis | Revenue by category bar, treemap, avg price bar | health_beauty #1, R$120 avg price, 71 categories |
| 🏪 Seller Analytics | Seller performance & delivery | Top sellers bar, state map, GMV trend by tier | 3,095 sellers, 92% on-time, 12 avg delivery days |

### Core DAX Measures Summary

| Measure | Result | Formula Summary |
|---------|--------|-----------------|
| GMV | R$ 16,008,872 | `SUM(fct_orders[TOTAL_REVENUE])` |
| Total Orders | 99,441 | `COUNTROWS(fct_orders)` |
| AOV | R$ 161 | `DIVIDE([GMV], [Total Orders])` |
| On-Time Delivery % | ~92% | Delivered + NOT late / Delivered orders × 100 |
| Avg Review Score | 4.09 | `AVERAGE(REVIEW_SCORE)` where `REVIEW_SCORE > 0` |
| Total Customers | 99,441 | `DISTINCTCOUNT(fct_orders[CUSTOMER_ID])` |
| Top Category By Revenue | health_beauty | `MAXX(TOPN(1, SUMMARIZE(..., "rev", SUM(PRICE)), [rev], DESC))` |

---

### ✅ Final Status: 37/37 dbt Tests Passing — Production Pipeline STABLE

---

## 📈 Key Business Questions Answered

| Page | Business Question | KPI / Metric |
|------|-------------------|--------------|
| Sales Overview | What is our total revenue and order volume? | GMV = R$16.01M, Orders = 99,441, AOV = R$161 |
| Sales Overview | How is revenue trending month over month? | Monthly GMV trend|
| Customer Insights | Where are our customers located? | São Paulo: 15,540 orders (top city) |
| Customer Insights | How satisfied are customers? | Avg Review Score = 4.09 / 5 |
| Product Performance | Which product categories generate most revenue? | health_beauty = R$1.26M (top category) |
| Product Performance | What is the average item price? | R$120.65 per item, 112,650 items sold |
| Seller Analytics | How are sellers performing on delivery? | On-Time Delivery = 92%|
| Seller Analytics | Which sellers drive most GMV? | Seller tier breakdown: micro/small/medium/large |

---

## 🎯 Project Goals

- Build a production-grade end-to-end data pipeline from raw CSV files to Power BI dashboards
- Implement the Medallion Architecture (Bronze → Silver → Gold) in Snowflake
- Apply modern data engineering best practices: dbt transformations, incremental loading, SCD Type 2
- Orchestrate the full pipeline with Apache Airflow running in Docker
- Enable business teams to self-serve analytics via Power BI with 4 dashboard pages

---

## 🏆 Project Summary

This project demonstrates the ability to design, build, test, and deploy a complete production data engineering pipeline from scratch — from raw data ingestion through orchestrated transformation to business-ready dashboards. Every architectural decision is documented, every error is resolved and explained, and every component follows modern data engineering best practices used at companies like Airbnb, Spotify, and Netflix.

**Scale:** 14 dbt models, 37 tests, 6 Snowflake schemas, 4 BI pages, 25 DAX measures, 10 Airflow tasks.

**Advanced Features:** Incremental loading, SCD Type 2, Snowflake clustering, dbt-expectations validation, GitHub Actions slim CI (`state:modified+`), custom Jinja macros, ephemeral intermediate models.

---

*Built by Kareem Basem Goda — Data Engineering Portfolio — June 2026*
