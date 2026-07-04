# Baby Names Lakehouse: Bronze to Gold with Databricks

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
The Gold layer provides business-ready datasets for reporting and analysis:

- Total births by year
- Total births by gender and year
- Top 10 baby names by year
- Most popular baby names across all years

These datasets support trend analysis, reporting, and downstream visualization tools.

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

## Visualizations

### Total Births by Year

screenshots/total_births_by_year.png

### Male vs Female Naming Trends

screenshots/gender_trends.png

### Top Baby Names (2015)

screenshots/top_names_2015.png

### Most Popular Names Across All Years

screenshots/top_names_all_time.png


## Project Structure

```text

baby-names-lakehouse-databricks
│
├── data
│   ├── yob2007.txt
│   ├── yob2008.txt
│   └── ...
│
├── notebooks
│   ├── bronze_ingestion.py
│   ├── silver_transformation.py
│   └── gold_analytics.py
│
├── screenshots
│   ├── volume_upload.png
│   ├── bronze_table.png
│   └── analytics.png
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
FROM silver_baby_names
GROUP BY year
ORDER BY year;
*``

---

## Learning Outcomes

Through this project I gained hands-on experience with:

- Databricks Lakehouse architecture
- Unity Catalog and Volumes
- Reading and transforming data with PySpark
- Delta Lake table creation
- Spark SQL analytics
- Medallion Architecture design patterns
- GitHub project documentation and version control

---

## Future Enhancements

- Implement Auto Loader for incremental ingestion
- Create Delta Live Tables pipeline - Build interactive dashboards
- Add data quality monitoring
- Automate pipeline orchestration

---

## Author

**Anna Grigoriadi**

Data Engineer with a passion for building scalable data solutions using Databricks, Microsoft Fabric, Azure, Spark, and modern Lakehouse architectures.
