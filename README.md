# Baby Names Lakehouse with Databricks

## Overview

This project demonstrates an end-to-end Data Engineering solution using Databricks, Apache Spark, and Delta Lake. The solution ingests raw US Baby Names data, transforms it through a Medallion Architecture (Bronze, Silver, and Gold layers), and enables analytics using Spark SQL.

The goal of this project is to showcase modern Lakehouse development practices, including data ingestion, transformation, storage, and analytical querying within Databricks.

---

## Architecture

### Bronze Layer
- Raw baby names data ingested from text files.
- Original source files stored in a Databricks Volume.
- Minimal transformations applied.
- Metadata captured for lineage and auditing.

### Silver Layer
- Data cleansing and standardization.
- Column renaming and schema enforcement.
- Year extracted from source filenames.
- Data quality validation performed.

### Gold Layer
- Business-ready datasets created for analytics.
- Aggregations and reporting tables generated.
- Supports trend analysis of baby names over time.

---

## Technologies Used

- Databricks Free Edition
- Apache Spark (PySpark)
- Delta Lake
- Unity Catalog
- Databricks Volumes
- Spark SQL
- GitHub

---

## Dataset

The project uses publicly available US Baby Names data.

Files loaded:

```text
yob2007.txt
yob2008.txt
yob2009.txt
yob2010.txt
yob2011.txt
yob2012.txt
yob2013.txt
yob2014.txt
yob2015.txt
```

Each file contains:

```text
Name, Gender, Count
```

Example:

```text
Emily,F,19355
Jacob,M,24210
```

---

## Project Structure

```text
baby-names-lakehouse-databricks
│
├── data/
│   ├── yob2007.txt
│   ├── yob2008.txt
│   ├── ...
│   └── yob2015.txt
│
├── notebooks/
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   └── gold_analytics.py
│
├── README.md
└── .gitignore
```

---

## Data Flow

```text
Raw Text Files
       │
       ▼
Bronze Layer
(Raw Ingestion)
       │
       ▼
Silver Layer
(Cleaned & Structured)
       │
       ▼
Gold Layer
(Business Analytics)
```

---

## Sample Analytics

Examples of analysis performed:

- Most popular baby names by year
- Male vs Female naming trends
- Top 10 names across all years
- Name popularity growth over time
- Year-over-year comparisons

Example SQL query:

```sql
SELECT
    year,
    COUNT(*) AS total_names
FROM silver_baby_*ames
GROUP BY year
ORDER BY year;
*``

---

## Learning Outcomes

Thr*ugh this project I gained hands-on*experience with:

- Databricks Lak*house architecture
- Unity Catalog*and Volumes
- Reading and transfor*ing data with PySpark
- Delta Lake*table creation
- Spark SQL analyti*s
- Medallion Architecture design *atterns
- GitHub project documenta*ion and version control

---

## F*ture Enhancements

- Implement Aut* Loader for incremental ingestion
* Create Delta Live Tables pipeline*- Build interactive dashboards
- A*d data quality monitoring
- Automa*e pipeline orchestration

---

## *uthor

**Anna Grigoriadi**

Data E*gineer Apprentice with a passion f*r building scalable data solutions*using Databricks, Microsoft Fabric* Azure, Spark, and modern Lakehous* architectures.
