# ETL Toll Data Project

Welcome to the **ETL Toll Data Project**, an end-to-end data pipeline designed to extract, transform, and load toll data from various sources into a unified, transformed dataset. This project leverages the power of **Apache Airflow** for orchestration, enabling automated data processing workflows.

---

## 🚀 Features

1. **Automated ETL Workflow**:
   - End-to-end data pipeline scheduled to run daily.
   - Modular and reusable Python functions for each ETL step.

2. **Data Sources Supported**:
   - CSV files for vehicle data.
   - TSV files for toll plaza details.
   - Fixed-width files for payment data.

3. **Data Transformation**:
   - Extract relevant fields from raw files.
   - Consolidate extracted data into a unified dataset.
   - Transform vehicle types to uppercase for standardization.

4. **Orchestrated by Apache Airflow**:
   - Clear DAG (Directed Acyclic Graph) structure.
   - Error handling with retry mechanisms.
   - Fully scheduled and scalable pipeline.

5. **Output**:
   - A clean and enriched dataset ready for analytics or further processing.

---

## 🛠️ Project Workflow

### 1. **Download Dataset**
- Downloads raw data (compressed `.tgz` file) from a public URL to the staging directory.

### 2. **Extract Data**
- **Untar Files**: Extracts the `.tgz` archive into the staging directory.
- **Parse Raw Files**:
  - Extract relevant fields from `vehicle-data.csv`, `tollplaza-data.tsv`, and `payment-data.txt`.

### 3. **Data Consolidation**
- Combines extracted data into a single, structured CSV file with the following fields:
  - Rowid
  - Timestamp
  - Anonymized Vehicle Number
  - Vehicle Type
  - Number of Axles
  - Toll Plaza ID
  - Toll Plaza Code
  - Type of Payment Code
  - Vehicle Code

### 4. **Data Transformation**
- Standardizes the `Vehicle Type` field by converting it to uppercase.

### 5. **Task Orchestration**
- Each step is defined as a task in an Airflow DAG, ensuring clear dependencies and seamless execution.

---

## 📂 Directory Structure

```
project/
|-- dags/
|   |-- ETL_toll_data.py       # Airflow DAG file
|-- staging/
|   |-- vehicle-data.csv       # Raw CSV file
|   |-- tollplaza-data.tsv     # Raw TSV file
|   |-- payment-data.txt       # Raw fixed-width file
|   |-- extracted_data.csv     # Consolidated dataset
|   |-- transformed_data.csv   # Final transformed dataset
```

---

## 📈 Results

The transformed dataset is clean, consolidated, and ready for downstream analysis. It provides a single source of truth for toll data, ensuring easier reporting and insights.

---

## 💻 Technologies Used

1. **Apache Airflow**:
   - DAG scheduling and task orchestration.

2. **Python**:
   - Data extraction and transformation.
   - Libraries: `os`, `csv`, `tarfile`, `requests`.

3. **File Formats**:
   - CSV, TSV, Fixed-width text files.

---



