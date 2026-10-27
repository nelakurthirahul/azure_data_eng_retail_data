# Part 2 — Metadata-Driven Incremental Data Pipeline

## Project Overview

Part 2 extends the Azure Retail Data Engineering project by implementing metadata-driven incremental processing using Azure Data Factory, Azure Databricks, PySpark, and Delta Lake.

The pipeline processes selected batches of retail transactions, merges changes into Silver tables, and incrementally updates Gold aggregations using Delta Change Data Feed (CDF).

## Technologies Used

- Azure Data Factory (ADF)
- Azure Databricks
- PySpark and Spark SQL
- Delta Lake and Delta Change Data Feed
- Azure Data Lake Storage Gen2
- Unity Catalog
- Git and GitHub

## Pipeline Architecture

```text
Retail Source Snapshot
         |
         v
Azure Data Factory
         |
         v
Metadata-Driven Batch Processing
         |
         v
Incremental Silver Layer
     (Delta MERGE)
         |
         v
Delta Change Data Feed
         |
         v
Incremental Gold Layer
         |
         v
Revenue Aggregation + Checkpoint
```

## Key Features

### 1. Metadata-Driven Processing

- Parameterized Databricks notebooks
- Batch-based source selection
- Batch tracking and processing status
- Data-quality validation

### 2. Incremental Silver Processing

- Delta Lake MERGE operations
- Insert and update handling
- Duplicate prevention using transaction identifiers
- Reprocessing protection for successfully completed batches

### 3. Incremental Gold Processing

- Delta Change Data Feed for identifying changed records
- Revenue aggregation using incremental adjustments
- Checkpoint-based version tracking
- Protection against reapplying previously processed changes

### 4. ADF Orchestration

Azure Data Factory orchestrates the Silver and Gold processing stages through Databricks notebook activities.

## Validation Results

The final development test produced:

| Metric | Result |
|---|---|
| Silver transactions | 202 |
| Gold transactions | 202 |
| Silver total revenue | 8,904.65 |
| Gold total revenue | 8,904.65 |
| Gold applied Delta version | 9 |
| Checkpoint Delta version | 9 |
| Reconciliation | PASS |

These values represent the Part 2 development test dataset, not the full Part 1 retail dataset.

## Notebooks

| Notebook | Purpose |
|---|---|
| 04_Metadata_Driven_Incremental_Retail | Batch selection, data validation, and incremental Silver MERGE |
| 05_Incremental_Silver_to_Gold_Retail | Development and testing of incremental Gold processing |
| 06_ADF_Incremental_Silver_to_Gold | Automated CDF-based Gold updates and checkpoint synchronization |

**Note:** Notebook 05 includes development and test operations and is not intended to run as an unattended production pipeline.

## Project Outcome

Implemented and validated an incremental data processing workflow with metadata-driven batch execution, Delta MERGE operations, Change Data Feed, and checkpoint-based processing.

The project demonstrates practical data engineering concepts including incremental ETL, data quality, orchestration, and pipeline reliability.
