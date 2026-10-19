# Azure Retail Data Engineering | Part 1

**End-to-end retail ETL with Azure Data Factory, Azure Databricks, PySpark, ADLS Gen2, Delta Lake, and Unity Catalog.**

![Retail data pipeline architecture](images/architecture.png)

> **Project scope:** This repository documents the implemented batch ETL pipeline. Incremental loading, metadata-driven configuration, automated data-quality gates, and audit logging are planned for Part 2.

## Overview

This project processes an Online Retail CSV dataset through a **Landing → Bronze → Silver → Gold** architecture using Azure Data Lake Storage Gen2.

Azure Data Factory orchestrates ingestion and two Databricks notebook activities. PySpark transforms transactional data and creates four business-oriented Gold datasets in Delta Lake format.

**536,641 retail transaction records** were processed into the Silver layer.

## Technologies

- **Azure Data Factory:** ETL orchestration and CSV ingestion
- **Azure Data Lake Storage Gen2:** Landing, Bronze, Silver, and Gold storage
- **Azure Databricks and PySpark:** Data cleaning, transformation, and aggregation
- **Delta Lake:** Silver and Gold storage
- **Unity Catalog:** Gold table registration and querying
- **SQL:** Data validation and revenue reconciliation
- **GitHub:** Version control and project documentation

## Architecture

![Azure Retail Architecture](images/architecture.png)

```text
Online Retail CSV
       |
       v
ADLS Gen2: Landing
       |
       | Azure Data Factory Copy Activity
       v
ADLS Gen2: Bronze (CSV)
       |
       | Databricks Notebook 01
       v
ADLS Gen2: Silver (Delta)
       |
       | Databricks Notebook 02
       v
ADLS Gen2: Gold (Delta)
       |
       v
Unity Catalog / SQL Analytics
```

## Azure Data Factory Pipeline

The ADF pipeline `PL_retail_Landing_To_Bronze` contains three sequential activities:

1. **Copy_Landing_To_Bronze:** Copies the retail CSV from Landing to Bronze.
2. **Notebook_Bronze_to_Silver:** Executes `01_Bronze_to_Silver_Retail` after the copy succeeds.
3. **Silver_to Gold:** Executes `02_Silver_to_Gold_Retail` after Silver processing succeeds.

Each activity runs after its predecessor succeeds.

## Silver Layer

The Silver dataset contains these columns:

| Column | Type |
|---|---|
| InvoiceNo | string |
| StockCode | string |
| Description | string |
| Quantity | int |
| InvoiceDate | timestamp |
| UnitPrice | double |
| CustomerID | string |
| Country | string |
| TransactionStatus | string |
| LineTotal | double |

## Gold Analytics Tables

| Table | Purpose |
|---|---|
| `gold_total_revenue` | Overall revenue |
| `gold_monthly_sales` | Monthly revenue trends |
| `gold_country_sales` | Revenue by country |
| `gold_top_products` | Top products by revenue |

## Validation Results

- Silver records: **536,641**
- Distinct full records: **536,641**
- Exact duplicate records detected: **0**
- Distinct invoice/product combinations: **531,225**
- Overall revenue: **9,726,006.95**
- Monthly revenue sum: **9,726,006.950000001**
- Country revenue sum: **9,726,006.950000007**

The Gold revenue totals agree to two decimal places. This establishes aggregate consistency, not exhaustive row-level validation.

### Revenue Reconciliation SQL

```sql
SELECT 'overall' AS source, SUM(TotalRevenue) AS revenue
FROM dbw_retail_dataeng.gold.gold_total_revenue
UNION ALL
SELECT 'monthly', SUM(MonthlyRevenue)
FROM dbw_retail_dataeng.gold.gold_monthly_sales
UNION ALL
SELECT 'country', SUM(TotalRevenue)
FROM dbw_retail_dataeng.gold.gold_country_sales;
```

## Reproducing the Pipeline

1. Provision Azure Data Factory, Azure Databricks, and ADLS Gen2.
2. Create Landing, Bronze, Silver, and Gold containers.
3. Upload the Online Retail CSV into Landing.
4. Configure ADF linked services, datasets, and permissions.
5. Import and configure the Databricks notebooks.
6. Run the ADF orchestration pipeline.
7. Query the Gold Delta tables and validate revenue totals.

The ADF reference JSON is not a standalone deployable infrastructure package. Azure resource configuration and permissions must be provided separately.

## Part 2 — Planned Enhancements

- Incremental data ingestion
- Stable transaction-line identifiers
- Delta Lake MERGE operations
- Automated data-quality checks
- Audit logging and pipeline monitoring
- Metadata-driven configuration
- Performance benchmarking

These are planned improvements and are not claimed as implemented in Part 1.

## Project Takeaway

This project demonstrates an end-to-end Azure data engineering workflow using ADF orchestration, PySpark transformations, Delta Lake storage, Unity Catalog, and SQL validation.

The next iteration will focus on improving incremental processing, reliability, and maintainability.
