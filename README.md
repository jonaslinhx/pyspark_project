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

# Python Dependencies
```
conda create -n pyspark_project python=3.10
conda activcate pyspark_project
pip install pipenv
pipenv shell
pipenv install # reads Pipfile in project directory
pipenv install specific_package
```

# Jenkins
- Jenkins is a automation server using the 1 master(controler) and a few workers concept
- Jenkins can be used for:
    - Pull Code
    - Compile & Build (Resolve Dependency)
    - Runs Unit Test
    - Check code quality (Package)
    - Deploy to QA
    - Runs Regression Test
    - Deploy to PROD
    - Automatically detect branch creation/deletion
- Multi branch pipeline:
    - Triggered by PR, Master branch will build, test, package, release, deploy
    - Triggered by PR, Release branch will build, test, package
    - Triggered by commits, Feature branches will only build and test
- Setup:
    - Configure Jenkins Github plugin to connect to Github source control
    - Configure Github webhooks to connect with Jenkins server
    - Create Jenkins file and code pipeline stages
    - Define multibranch pipelines in Jenkins

# Kafka
- Kafka only accepts key-value dataframe
- Data sent to kafka has to be a 2 column dataframe: first column is the key and second column is the value (json string)
- A topic with 6 partitions

# Unit Testing
- Create test file
    - File name must start with `test_` or end with `_test.py`
- Import pytest package
- Create pytest fixture
    - Fixtures are variables or functions that we need to provide context for tests.
    - Fixtures have scope to define the extent of reusability.
- Create pytest function

# Resource Planning
- How many cpu and memory needed for spark application.
    - spark.driver.cores -> How much cpu cores to allocate for driver container
    - spark.driver.memory -> How much JVM Heap memory to allocate to driver container
    - spark.driver.memoryOverhead -> How much non-JVM off-heap memory to allocate to driver container
    - spark.executor.cores ->
    - spark.executor.memory
        - spark.memory.fraction
        - spark.memory.storageFraction
        - spark.python.worker.memory
    - spark.executor.memoryOverhead
        - spark.executor.memoryOverheadFactor
        - spark.executor.pyspark.memory
    - spark.executor.instances
- spark driver configurations defined in `spark-submit.sh`
- spark executor configurations defined in application `spark.conf`