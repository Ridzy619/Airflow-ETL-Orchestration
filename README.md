# Airflow-ETL-Orchestration
This project shows how ETL pipelines can be created with Airflow DAGs

The project makes use of four custom plugins:
1. stage_redshift: to stage data on redshift,
2. load_dimension: to load dimension data from redshit,
3. load_fact: to load the fact table from redshift,
4. data_quality: to validate and test the final tables for consistencies.


The pipelines are well parametarized for a greater flexibility. The ETL pipeline is set-up to run back-fill based on the start date of the schedule. Retries are also configured to ensure failures are retried.
