<div align="center">

# 🔷 Azure End-to-End Data Engineering Pipeline

### A production-style cloud data pipeline built from scratch using Microsoft Azure

![Azure](https://img.shields.io/badge/Microsoft_Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

**Status: ✅ Complete | Pipeline Run: Succeeded (7m 36s) | Region: Central India**

</div>

---

## 📌 What Is This Project?

This is a **self-built, end-to-end data engineering pipeline on Microsoft Azure** — designed to simulate how real companies move raw data from source systems all the way to live business dashboards.

The project covers every layer of a modern data stack:
- **Ingestion** → pulling raw data from GitHub into Azure SQL using Azure Data Factory
- **Storage** → organizing data across Bronze, Silver, and Gold zones in ADLS Gen2
- **Transformation** → cleaning, standardizing, and modeling data using PySpark on Databricks
- **Serving** → exposing a Star Schema in Databricks Catalog for live Power BI reporting

Everything was provisioned, built, and tested by me — no templates, no tutorials copied.

---

## 🏗️ Full Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                                  │
│                                                                     │
│                    GitHub (Raw CSV/Parquet)                         │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  AZURE DATA FACTORY (consolazf)                     │
│                                                                     │
│   Pipeline 1: git_to_sql                                            │
│   └── Copy Data: GitHub → Azure SQL Database                        │
│                                                                     │
│   Pipeline 2: Incremental_Pipeline                                  │
│   └── Lookup(lastdate) ──┐                                          │
│       Lookup(maxdate)  ──┴──► Copy Data ──► Stored Procedure        │
│                               (newdata)      (updatedateholders)    │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ADLS GEN2 — cfstorageaccn (3 Containers)               │
│                                                                     │
│   🥉 bronze/   →  Raw Parquet files, as-is from source              │
│   🥈 silver/   →  Cleaned & standardized data                       │
│   🥇 gold/     →  Star Schema Delta tables (final serving layer)    │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│            AZURE DATABRICKS (cfdatabricks) — PySpark                │
│                                                                     │
│   Silver.ipynb         →  Bronze to Silver transformation           │
│   GoldDimCustomer.ipynb→  Build dim_Customer (Delta + Merge)        │
│   GoldDimDate.ipynb    →  Build dim_Date (Delta + Merge)            │
│   GoldDimProduct.ipynb →  Build dim_Product (Delta + Merge)         │
│   GoldDimRegion.ipynb  →  Build dim_Region (Delta + Merge)          │
│   Fact_Sales.ipynb     →  Build dim_factsales (central fact table)  │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│         DATABRICKS SQL WAREHOUSE — Star Schema (5 Tables)           │
│                                                                     │
│   dim_customer  |  dim_date  |  dim_product  |  dim_region          │
│                       dim_factsales (Fact)                          │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    POWER BI DESKTOP                                  │
│                                                                     │
│   Live connection via SQL Warehouse token + HTTP Path               │
│   Zero intermediate exports — data always fresh                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ☁️ Azure Resources Provisioned

All resources are deployed in **Central India** region under resource group `cfresource`.

| Resource Name | Service | Purpose |
|---|---|---|
| `cfresource` | Resource Group | Container for all project resources |
| `cfstorageaccn` | ADLS Gen2 Storage Account | Bronze / Silver / Gold data lake layers |
| `cfdatabricks` | Azure Databricks Service | PySpark notebooks and SQL Warehouse |
| `consolazf` | Azure Data Factory V2 | Pipeline orchestration and scheduling |
| `consoleserv` | Azure SQL Server | Source database server |
| `cfdatabaes` | Azure SQL Database | Raw data landing table for incremental load |

---

## 🔄 ADF Pipelines — Detailed Explanation

### Pipeline 1: `git_to_sql`

**What it does:**
This pipeline pulls raw data from a GitHub repository and loads it into Azure SQL Database. It acts as the entry point for data into the pipeline.

**How it works:**
- **Source:** GitHub linked service using HTTP connector — points to raw CSV/Parquet file URL on GitHub
- **Activity:** `Copy Data` → activity name: `github_to_sqltable`
- **Sink:** Azure SQL Database (`sqldb_link_service`) — writes data into the landing table
- **When to run:** Manually triggered, or scheduled when new source data is available

**Why this approach:**
Using GitHub as a data source is a lightweight, version-controlled way to manage raw data files. In a real company this would be replaced by an on-premise system, API, or S3 bucket — but the ADF connector pattern is identical.

---

### Pipeline 2: `Incremental_Pipeline`

**What it does:**
This is the core ingestion pipeline. Instead of reloading all data every run (Full Load), it uses a **watermark pattern** to load only new records that arrived since the last run. This is how production pipelines work in real companies.

**How it works — step by step:**

```
Step 1: Lookup Activity — "lastdate"
        Reads the watermark table in Azure SQL
        Returns: the last successfully loaded date (e.g. 2024-01-15)

Step 2: Lookup Activity — "maxdate"  
        Queries the source table for the maximum date currently in source
        Returns: the latest date in source data (e.g. 2024-01-20)

Step 3: Copy Data Activity — "newdata"
        Copies only records where date > lastdate AND date <= maxdate
        These are the NEW records that haven't been loaded yet

Step 4: Stored Procedure Activity — "updatedateholders"
        After successful copy, updates the watermark table
        Sets lastdate = maxdate so next run starts from here
```

**Why Incremental Load matters:**
- Avoids reprocessing millions of historical rows on every run
- Reduces Azure compute cost significantly
- Makes pipelines faster and more reliable
- Industry standard pattern used by all data engineering teams

---

## ⚡ Databricks Notebooks — What Each One Does

### `Silver.ipynb` — Bronze → Silver Transformation

**Purpose:** Take raw, messy Parquet data from Bronze and produce a clean, standardized dataset in Silver.

**What the code does:**

```
1. Connect to ADLS Gen2 Bronze container
2. Read raw Parquet file into a Spark DataFrame
3. Data Quality Transformations:
   - initcap() on Country        → "india" becomes "India"
   - initcap() on ProductCategory → standardizes category names
   - initcap() on ProductName     → standardizes product names
   - initcap() on SalesRegion     → standardizes region names
   - initcap() on CustomerName    → standardizes customer names
4. dropna(how='all')    → removes completely empty rows
5. dropDuplicates()     → removes exact duplicate records
6. Write clean data to Silver container as Parquet (overwrite mode)
```

**Why Silver layer exists:**
Raw data from source systems is always dirty — inconsistent casing, nulls, duplicates. Silver is the "single source of truth" — clean enough to build business tables from, but not yet modeled into a specific schema.

---

### `GoldDimCustomer.ipynb` — dim_Customer Dimension Table

**Purpose:** Build and maintain the Customer dimension table in the Gold layer using Delta Lake.

**What the code does:**

```
1. Read Silver layer Parquet data
2. Extract distinct customers:
   SELECT DISTINCT CustomerID, CustomerName, Country FROM silver

3. Check if dim_Customer Delta table already exists in Gold:
   - If EXISTS  → read existing table (df_sink)
   - If NOT EXISTS → create empty DataFrame with same schema

4. Join source (df_src) with existing table (df_sink) on CustomerID:
   - df_old = customers that already exist (Dim_Customer_Key is NOT NULL)
   - df_new = brand new customers (Dim_Customer_Key IS NULL)

5. Generate Surrogate Keys for new customers:
   - If Incremental_Flag = '0' (first run) → start from 1
   - If Incremental_Flag = '1' (subsequent run) → start from max existing key + 1
   - Uses monotonically_increasing_id() to assign unique integer keys

6. Union df_old + df_new_with_keys → complete dimension table

7. Write to Gold using Delta MERGE (UPSERT):
   whenMatchedUpdateAll()    → update existing customer if data changed
   whenNotMatchedInsertAll() → insert brand new customer
```

**Key concept — Surrogate Keys:**
Instead of using CustomerID (which is a business key and can change), we generate a separate integer `Dim_Customer_Key`. This is the standard dimensional modeling practice — the fact table joins to the dimension using this surrogate key.

**Key concept — Delta MERGE:**
Delta Lake's MERGE operation is like a SQL UPSERT — it checks if the record exists, updates it if yes, inserts it if no. This is what makes the table reliable and ACID-compliant (Atomicity, Consistency, Isolation, Durability).

---

### `GoldDimDate.ipynb` — dim_Date Dimension Table

**Purpose:** Build a Date dimension table that allows time-based analysis (by Year, Month, Day).

**What the code does:**

```
1. Read Silver layer Parquet data
2. Extract distinct order dates:
   SELECT DISTINCT OrderDate FROM silver

3. Derive time columns using PySpark date functions:
   - year(OrderDate)  → extracts year  (e.g. 2024)
   - month(OrderDate) → extracts month (e.g. 1 to 12)
   - day(OrderDate)   → extracts day   (e.g. 1 to 31)

4. Check if dim_Date Delta table exists:
   - If EXISTS → read from Delta
   - If NOT EXISTS → create empty DataFrame with schema definition

5. Join source vs existing on OrderDate
   - Identify new dates vs already-existing dates

6. Generate surrogate key Dim_Date_Key

7. Delta MERGE UPSERT into Gold
```

**Why a Date dimension:**
Without a date dimension, you can't easily filter by month, quarter, or year in Power BI. The Date dimension table is one of the most important tables in any data warehouse — it's what enables time intelligence in DAX.

---

### `GoldDimProduct.ipynb` — dim_Product Dimension Table

**Purpose:** Build the Product dimension table.

**What the code does:**

```
1. Read Silver Parquet data
2. Extract distinct products:
   SELECT DISTINCT ProductID, ProductCategory, ProductName, UnitPrice FROM silver

3. Check if dim_Product Delta table exists in Gold
4. Join source vs existing on ProductID
5. Split into df_old (existing) and df_new (new products)
6. Generate surrogate key Dim_Product_Key
7. Delta MERGE UPSERT
```

**Columns in dim_Product:**
- `Dim_Product_Key` — surrogate key (integer)
- `ProductID` — business/natural key
- `ProductCategory` — e.g. Electronics, Clothing
- `ProductName` — e.g. Laptop, T-Shirt
- `UnitPrice` — price per unit

---

### `GoldDimRegion.ipynb` — dim_Region Dimension Table

**Purpose:** Build the Region dimension table for geographic analysis.

**What the code does:**

```
1. Read Silver Parquet data
2. Extract distinct regions:
   SELECT DISTINCT SalesRegion FROM silver

3. Check if dim_Region Delta table exists in Gold
4. Join source vs existing on SalesRegion
5. Split into df_old and df_new
6. Generate surrogate key Dim_Region_Key
7. Delta MERGE UPSERT
```

---

### `Fact_Sales` (Databricks Workspace) — Central Fact Table

**Purpose:** Join all dimension tables with the raw sales transactions to produce the central Fact table.

**Output table:** `dim_factsales`

**What it contains:**
- Foreign keys to all 4 dimension tables (Dim_Customer_Key, Dim_Date_Key, Dim_Product_Key, Dim_Region_Key)
- Measures: Quantity, Revenue, Discount, Total Amount etc.
- This is the table Power BI queries to build sales reports

---

## 📐 Star Schema Design

```
                        ┌───────────────────────┐
                        │     dim_customer       │
                        │─────────────────────── │
                        │ Dim_Customer_Key (PK)  │
                        │ CustomerID             │
                        │ CustomerName           │
                        │ Country                │
                        └───────────┬───────────┘
                                    │
    ┌──────────────────┐    ┌───────▼──────────────┐    ┌──────────────────────┐
    │    dim_date      │    │    dim_factsales      │    │    dim_product       │
    │──────────────────│    │──────────────────────│    │──────────────────────│
    │ Dim_Date_Key(PK) ├────► Dim_Date_Key (FK)    │    │ Dim_Product_Key (PK) │
    │ OrderDate        │    │ Dim_Customer_Key (FK) ◄────┤ ProductID            │
    │ Year             │    │ Dim_Product_Key (FK)  │    │ ProductCategory      │
    │ Month            │    │ Dim_Region_Key (FK)   ◄──┐ │ ProductName          │
    │ Day              │    │ Quantity              │  │ │ UnitPrice            │
    └──────────────────┘    │ Revenue               │  │ └──────────────────────┘
                            │ TotalAmount           │  │
                            └───────────────────────┘  │ ┌──────────────────────┐
                                                        │ │    dim_region        │
                                                        │ │──────────────────────│
                                                        └─┤ Dim_Region_Key (PK)  │
                                                          │ SalesRegion          │
                                                          └──────────────────────┘
```

**Why Star Schema:**
- Simplest schema for BI tools to query — Power BI loves it
- Fast query performance — fewer joins needed
- Easy for business users to understand
- Industry standard for data warehouses

---

## 📊 Power BI Live Connection

**Connection Type:** Azure Databricks SQL Warehouse (DirectQuery / Import)

**Steps performed:**
1. Power BI Desktop → Get Data → Azure Databricks
2. Server Hostname: `adb-xxxxx.azuredatabricks.net`
3. HTTP Path: `/sql/1.0/warehouses/<warehouse_id>`
4. Authentication: Personal Access Token (PAT)
5. Selected tables: `dim_customer`, `dim_date`, `dim_product`, `dim_region`, `dim_factsales`

**Result:** Live Gold layer reporting — Power BI reads directly from Databricks SQL Warehouse. No intermediate CSV exports, no scheduled refresh of flat files — data is always current.

---

## 🎯 Key Data Engineering Concepts Demonstrated

| Concept | Where Used | Why It Matters |
|---|---|---|
| **Medallion Architecture** | ADLS Bronze→Silver→Gold | Industry standard for organizing data lake layers |
| **Incremental Load** | ADF Incremental_Pipeline | Avoids full reload — only processes new records |
| **Watermark Pattern** | ADF Lookup + Stored Procedure | Tracks last loaded date to determine what's new |
| **Star Schema** | Gold layer (4 Dim + 1 Fact) | Optimized structure for BI reporting |
| **Surrogate Keys** | All dimension tables | Stable integer keys independent of source system |
| **Delta Lake** | All Gold tables | ACID transactions, time travel, schema enforcement |
| **Delta MERGE (UPSERT)** | All Gold Dim notebooks | Update existing + insert new in single operation |
| **SCD Type 1** | Dim tables via MERGE | Overwrites old values with new (no history kept) |
| **Data Cleaning** | Silver notebook | Null removal, deduplication, string standardization |
| **Surrogate Key Generation** | `monotonically_increasing_id()` | Auto-increment integer keys in distributed Spark |
| **Incremental Flag Widget** | All Gold notebooks | Controls first-run vs incremental-run behavior |
| **Live BI Connection** | Power BI → SQL Warehouse | No data export needed — queries Gold layer directly |

---

## 📁 Repository Structure

```
azure-end-to-end-data-pipeline/
│
├── README.md                    ← You are here
│
├── notebooks/
│   ├── Silver.ipynb             ← Bronze → Silver transformation
│   ├── GoldDimCustomer.ipynb    ← dim_Customer dimension table
│   ├── GoldDimDate.ipynb        ← dim_Date dimension table
│   ├── GoldDimProduct.ipynb     ← dim_Product dimension table
│   └── GoldDimRegion.ipynb      ← dim_Region dimension table
│
└── screenshots/                 ← (Add your project screenshots here)
    ├── 01_resource_group.png
    ├── 02_adls_containers.png
    ├── 03_adf_incremental_pipeline.png
    ├── 04_databricks_pipeline_run.png
    ├── 05_databricks_catalog_tables.png
    └── 06_powerbi_connection.png
```

---

## ✅ Project Completion Checklist

- [x] Azure Resource Group provisioned with all 5 services
- [x] ADLS Gen2 — Bronze, Silver, Gold containers created
- [x] ADF Pipeline 1 — GitHub to Azure SQL (git_to_sql)
- [x] ADF Pipeline 2 — Incremental Load with watermark pattern
- [x] Databricks Silver notebook — data cleaning and standardization
- [x] Databricks Gold notebooks — all 4 dimension tables with Delta MERGE
- [x] Databricks Fact_Sales notebook — central fact table
- [x] Databricks pipeline run — Succeeded (Duration: 7m 36s)
- [x] Star Schema live in Databricks Catalog (5 tables)
- [x] Power BI connected live via SQL Warehouse token

---

## 👤 About

**Hemant Yadav** — Data Analyst & Azure Data Engineer  
📍 Mathura, Uttar Pradesh, India  
🔗 [LinkedIn](https://linkedin.com/in/hemant-yadav-273a60376)

*This project was built entirely on personal initiative to develop hands-on cloud data engineering skills alongside 5+ years of professional data analytics experience.*
