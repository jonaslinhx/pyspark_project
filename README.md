# Introduction
- This project is a batch processing data pipeline demonstrating the respective skills:
    - Data Engineering
        - pyspark
    - DevOps
        - CI/CD

# Objective

[MDM Platform] -> Kafka Cluster] daily -> [Service System] such as Customer Churn, Fraud Detection
Batch Data Flow Pipeline

# Context
- A company has a Master Data Management (MDM) platform that maintains accurate copy of entity data across Business Units (LOB - Lines of Business)

- Full data load of entity data is exported Hive (big data hadoop cluster) daily and Hive tables are partitioned by `load_date`.

- The hadoop cluster maintains 7 days of exports.

- Data is processed and sent to Kafka topics for downstream service systems to consume. 
- Downstream service systems, such as Fraud Detection, Customer Churn, requires local copy of data to generate BI reports, dashboards and predictions.

# Objective
- Create a spark application that:
    1. Takes `load_date` as an argument
    2. Process data according:
        - Aggregate
    3. Sends data to kafka topic
    4. Estimate computational requirements such as `num_executors`, `cpu`, and `memory`.
    5. Setup CI/CD pipeline.

# Data Exploration
| Accounts | Parties | Party Address |
| :-- | :-- | :--
| load_date | load_date | load_date
| active_ind | account_id | party_id
| account_id | party_id | address_line_1
| source_sys | relation_start_date | address_line_2
| account_start_date | | city
| legal_title_1 | | postal_code
| legal_title_2 | | country_of_address
| tax_id_type | | address_start_date
| tax_id |
| branch_code | 
| country |
## Accounts Table
1. 
## Parties Table
## Party Address Table