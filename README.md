🚓 Modern Data Warehouse using Crime Analytics

Welcome to the Modern Data Warehouse using Crime Analytics repository! 🚀
This project showcases a complete data warehousing and analytics solution—from data ingestion to generating actionable insights. Designed as a portfolio project, it demonstrates industry-standard best practices in data engineering and analytics.

🏗️ Data Architecture

This project implements the Medallion Architecture, using Bronze, Silver, and Gold layers:

Bronze Layer: Stores raw data ingested from CSV files (ERP and CRM systems) into SQL Server.

Silver Layer: Performs data cleansing, transformation, and standardization for analytical readiness.

Gold Layer: Contains refined, business-ready data modeled into a star schema for reporting and dashboarding.

📖 Project Overview

Key Components:
Data Architecture: Modern Data Warehouse with Medallion Layering.

ETL Pipelines: Extract, Transform, Load workflows using SQL Server.

Data Modeling: Star schema design with fact and dimension tables.

Analytics & Reporting: SQL-based insights into customer, product, and sales behavior.

🚀 Project Goals

🧱 Data Warehouse (Data Engineering)

Objective: Build a scalable warehouse in SQL Server to unify and analyze sales and customer data.

Key Features:

📂 Sources: ERP and CRM data provided as CSV files.

🧹 Data Quality: Cleaning and resolving inconsistencies.

🔗 Integration: Unified, user-friendly model for analytical queries.

⏳ Scope: Focused on the latest snapshot (no historization).

📄 Documentation: Well-documented data models for both business and tech teams.

📊 BI & Reporting (Data Analysis)

Objective: Generate insights using SQL queries and dashboards for:

👥 Customer Behavior

📦 Product Performance

📈 Sales Trends

These insights help drive data-driven decision-making for stakeholders.

📂 Repository Structure

data-warehouse-project/

│

├── datasets/                           # Raw ERP and CRM data (CSV)

│

├── docs/                               # Documentation and architecture diagrams

│   ├── etl.drawio

│   ├── data_architecture.drawio

│   ├── data_flow.drawio

│   ├── data_models.drawio

│   ├── data_catalog.md                 # Dataset field descriptions & metadata

│   ├── naming-conventions.md           # Naming standards for consistency

│

├── scripts/                            # SQL scripts for ETL & transformations

│   ├── bronze/                         # Raw data ingestion

│   ├── silver/                         # Data cleaning & transformation

│   ├── gold/                           # Star schema & analytical models

│
├── tests/                              # Data validation and test scripts

│

├── README.md                           # Project overview and guide

👩‍💻 About Me
Krimisha Vaghela

Aspiring Data Engineer | Passionate about transforming raw data into business insights

[LinkedIn] [https://www.linkedin.com/in/krimisha-vaghela13/]

[GitHub] [https://github.com/krimisha13/]
