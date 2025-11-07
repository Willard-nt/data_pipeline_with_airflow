# Data Pipeline with Apache Airflow

This project implemented a data pipeline using **Apache Airflow** to orchestrate ETL operations from **Amazon S3** to **Amazon Redshift**. The pipeline was designed to stage, transform, and validate data for a music streaming analytics platform.

---

## Project Files

You can explore the individual components of the project here:  
- [`dags/`](./dags) – DAG definition and task configuration  
- [`plugins/operators/`](./plugins/operators) – Custom operator implementations  
- [`plugins/helpers/sql_queries.py`](./plugins/helpers/sql_queries.py) – SQL transformation helper class

---

## Project Overview

The project was built using a provided template that included:
- A DAG definition file with task placeholders
- Custom operator templates for staging, loading, and data quality checks
- A helper class for SQL transformations

The DAG was visualized and executed within the Airflow UI.  
**Visualization Placeholder:**  
![Visual of DAG graph](.png)

---

## Setup and Data Preparation

### S3 Bucket Configuration

To ensure Redshift could access the data efficiently, the datasets were copied from Udacity’s public S3 bucket to a user-owned bucket in the same AWS region.

#### Steps Taken:
```bash
# Create a new S3 bucket
aws s3 mb s3://your-unique-bucket-name/

# Copy data from Udacity's bucket to CloudShell
aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json ~/

# Upload data to your own S3 bucket
aws s3 cp ~/log-data/ s3://your-unique-bucket-name/log-data/ --recursive
aws s3 cp ~/song-data/ s3://your-unique-bucket-name/song-data/ --recursive
aws s3 cp ~/log_json_path.json s3://your-unique-bucket-name/
```
## DAG Configuration
The DAG was configured with the following default parameters:

No dependency on past runs

Retries: 3 attempts

Retry delay: 5 minutes

Catchup: Disabled

Email on retry: Disabled

## Final DAG Structure
The task dependencies were set as follows:

Begin_execution → create_tables

create_tables → stage_events and stage_songs

stage_events, stage_songs → Load_songplays_fact_table

Load_songplays_fact_table →

Load_user_dim_table

Load_artist_dim_table

Load_song_dim_table

Load_time_dim_table

All dimension loads → Run_data_quality_checks

Run_data_quality_checks → End_execution

![Dependencies](.png)

## Custom Operators
1. CreateTablesOperator
Executed SQL scripts to initialize Redshift tables before staging began.

2. StageToRedshiftOperator
Loaded JSON-formatted files from S3 to Redshift using the COPY command.

Included templated fields for timestamped file loading and backfills.

3. LoadFactOperator
Inserted data into the songplays fact table using SQL transformations.

Supported append-only mode for large datasets.

4. LoadDimensionOperator
Loaded data into dimension tables (users, songs, artists, time).

Supported both append and truncate-insert modes via parameters.

5. DataQualityOperator
Ran SQL-based test cases to validate data integrity.

Raised exceptions if test results did not match expected values.

## Execution Results
All tasks were successfully executed in Airflow. Each task showed a green "Success" status in the DAG graph view.

![Visual of succesful DAG graph](.png)

## Notes
The project reused logic from previous ETL implementations but leveraged Airflow's hooks and connections for modularity.

SQL queries were abstracted using a helper class for maintainability.

The pipeline was designed to be reusable for other Redshift-based workflows.

## Final Thoughts
This project demonstrated how to build a robust, scalable ETL pipeline using Airflow and AWS services. The modular design of operators and clear DAG structure made it easy to monitor, debug, and extend.
