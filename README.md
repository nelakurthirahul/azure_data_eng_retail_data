# Azure Retail Data Engineering | End-to-End Batch & Incremental ETL

**An end-to-end Azure data engineering project implementing batch ETL, metadata-driven incremental processing, Delta Lake MERGE operations, Change Data Feed (CDF), and automated orchestration using Azure Data Factory, Azure Databricks, PySpark, ADLS Gen2, and Unity Catalog.**

## Project Overview

This project demonstrates the design and implementation of a cloud-based retail data engineering platform using Microsoft Azure.

The solution is divided into two parts:

- **Part 1 — Batch Data Processing:** Ingests an Online Retail CSV dataset, cleans and transforms transactions, and generates business-oriented Gold analytics tables.
- **Part 2 — Incremental Data Processing:** Extends the architecture with metadata-driven batch processing, Delta MERGE, Change Data Feed, checkpoint tracking, and incremental revenue aggregation.

The project demonstrates practical data engineering concepts including ETL orchestration, Medallion Architecture, data quality, incremental processing, and data reconciliation.

## Technologies Used

| Technology | Purpose |
|---|---|
| Azure Data Factory | Pipeline orchestration and ingestion |
| Azure Databricks | Distributed data processing |
| PySpark | Data cleaning, transformation, and aggregation |
| Azure Data Lake Storage Gen2 | Cloud data storage |
| Delta Lake | Transactional tables and incremental processing |
| Delta Change Data Feed | Tracking changed records |
| Unity Catalog | Data governance and table management |
| SQL | Validation, reconciliation, and analytics |
| GitHub | Version control and documentation |

## Part 1 — Batch Data Processing

### Architecture

```text
Online Retail CSV
       |
       v
ADLS Gen2 — Landing
       |
       | Azure Data Factory Copy Activity
       v
ADLS Gen2 — Bronze (CSV)
       |
       | Databricks Notebook 01
       v
ADLS Gen2 — Silver (Delta)
       |
       | Databricks Notebook 02
       v
ADLS Gen2 — Gold (Delta)
       |
       v
Unity Catalog / SQL Analytics
```

### Implementation

The batch pipeline follows the Landing → Bronze → Silver → Gold architecture.

**Landing and Bronze**

- Ingested the Online Retail CSV dataset through Azure Data Factory.
- Stored source records in ADLS Gen2.
- Preserved the original source data for downstream processing.

**Silver**

- Applied PySpark transformations and data type conversions.
- Standardized transaction timestamps.
- Removed exact duplicate records.
- Added `TransactionStatus` and `LineTotal` columns.
- Stored the cleaned dataset in Delta Lake format.

**Gold**

Created four analytics datasets:

| Gold Table | Purpose |
|---|---|
| `gold_total_revenue` | Overall retail revenue |
| `gold_monthly_sales` | Monthly sales trends |
| `gold_country_sales` | Revenue by country |
| `gold_top_products` | Top-performing products |

### Part 1 Results

| Metric | Result |
|---|---:|
| Bronze records | 541,909 |
| Silver records | 536,641 |
| Duplicate records removed | 5,268 |
| Remaining exact duplicates | 0 |
| Overall revenue | 9,726,006.95 |
| Monthly revenue reconciliation | PASS |
| Country revenue reconciliation | PASS |

Revenue totals reconcile to two decimal places.

### Revenue Validation SQL

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

## Part 2 — Metadata-Driven Incremental Data Processing

### Objective

Part 2 extends the batch pipeline by implementing incremental data processing to avoid unnecessarily reprocessing previously handled records.

It introduces:

- Metadata-driven batch selection
- Parameterized Databricks notebooks
- Delta Lake MERGE operations
- Delta Change Data Feed
- Incremental Gold revenue updates
- Checkpoint and batch tracking
- Data-quality validation
- Azure Data Factory orchestration

### Incremental Pipeline Architecture

```text
Retail Source Snapshot
         |
         v
Azure Data Factory
         |
         v
Metadata-Driven Batch Selection
         |
         v
Incremental Silver Processing
      (Delta MERGE)
         |
         v
Delta Change Data Feed (CDF)
         |
         v
Incremental Gold Processing
         |
         v
Revenue Aggregation
         |
         v
Checkpoint Synchronization
         |
         v
SQL Validation & Reconciliation
```

### Metadata-Driven Processing

The incremental Silver notebook uses configurable parameters:

- `batch_id`
- `source_table`
- `target_table`
- `tracking_table`

The pipeline selects the appropriate source records, performs validation, and executes a Delta MERGE into the Silver target table.

Successfully completed batches are tracked to prevent unnecessary reprocessing.

### Incremental Silver Processing

Delta Lake MERGE supports inserting new transactions and updating existing records based on transaction identifiers.

This approach helps maintain a consistent Silver dataset while supporting incremental processing.

### Delta Change Data Feed

Delta Change Data Feed identifies changes in the Silver Delta table.

The Gold processing notebook reads changes since the previously applied Delta version and calculates incremental adjustments.

The implementation handles:

- Inserted records
- Deleted records
- Updated records using pre-image and post-image changes

### Incremental Gold Aggregation

The Gold processing stage updates cumulative revenue and transaction counts without recalculating the complete Silver dataset for every run.

A checkpoint mechanism records the last successfully applied Delta version.

The implementation also checks previously applied versions to avoid reapplying the same changes during sequential retries.

### Azure Data Factory Orchestration

The incremental pipeline uses Azure Data Factory to execute Databricks notebooks in sequence.

**Pipeline:** `PL_Metadata_Driven_Incremental_Retail`

Main activities:

1. `Execute_Incremental_Retail_Notebook` — Incremental Silver processing.
2. `Execute_Incremental_Gold_Notebook` — CDF-based Gold processing and checkpoint synchronization.

### Part 2 Validation Results

| Metric | Result |
|---|---:|
| Silver transactions | 202 |
| Gold transactions | 202 |
| Silver total revenue | 8,904.65 |
| Gold total revenue | 8,904.65 |
| Gold applied Delta version | 9 |
| Checkpoint Delta version | 9 |
| Reconciliation | PASS |

**Note:** Part 2 uses a separate development test dataset. These results should not be directly compared with the full Part 1 retail dataset.

## Repository Structure

```text
azure_data_eng_retail_data/
|
|-- 01_Bronze_to_Silver_Retail.ipynb
|-- 02_Silver_to_Gold_Retail.ipynb
|-- Architecture.png
|-- PL_retail_Landing_To_Bronze.json
|-- README.md
|
|-- part_2_incremental_processing/
    |-- 04_Metadata_Driven_Incremental_Retail.ipynb
    |-- 05_Incremental_Silver_to_Gold_Retail.ipynb
    |-- 06_ADF_Incremental_Silver_to_Gold.ipynb
    |-- README.md
```

*File extensions and paths should match the files in the repository.*

Notebook 05 contains development and testing operations and is not intended for unattended production execution.

## Data Quality and Reliability

The project incorporates several data engineering practices:

- Data type validation and transformation
- Duplicate detection and removal
- Transaction-level revenue calculations
- Delta MERGE for incremental updates
- Batch tracking and reprocessing checks
- Delta version checkpointing
- Revenue and transaction-count reconciliation

These features improve the reliability and maintainability of the implemented workflows.

## Reproducing the Project

1. Provision Azure Data Factory, Azure Databricks, and Azure Data Lake Storage Gen2.
2. Configure the Landing, Bronze, Silver, and Gold storage containers.
3. Upload the Online Retail dataset to the Landing container.
4. Configure Azure permissions, linked services, and Unity Catalog.
5. Import and configure the Databricks notebooks.
6. Run the Part 1 batch pipeline.
7. Configure the Part 2 source snapshot, Delta tables, and batch tracking.
8. Enable Change Data Feed for the relevant Silver table.
9. Configure and execute the incremental ADF pipeline.
10. Validate Silver and Gold transaction counts, revenue totals, and checkpoints.

The repository provides implementation examples and documentation, but is not a fully automated infrastructure deployment package.

## Key Technical Achievements

- Built a batch ETL pipeline using Azure Data Factory and Azure Databricks.
- Processed more than 540,000 source retail records.
- Implemented Medallion Architecture using ADLS Gen2 and Delta Lake.
- Developed business-oriented Gold analytics tables.
- Implemented metadata-driven incremental Silver processing.
- Used Delta MERGE for inserting and updating records.
- Implemented Delta Change Data Feed for incremental Gold aggregation.
- Added checkpoint-based processing and batch tracking.
- Validated transaction counts and revenue reconciliation.

## Project Outcome

This project demonstrates practical experience designing, implementing, orchestrating, and validating cloud-based data pipelines using Microsoft Azure.

By combining batch and incremental processing, it showcases data engineering skills relevant to Azure Data Engineer and Junior Data Engineer roles.
