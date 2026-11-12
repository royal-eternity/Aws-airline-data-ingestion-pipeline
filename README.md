# Airline Data Ingestion Project

---

## Project Overview

This project demonstrates an event-driven airline data ingestion pipeline built using AWS services.

The pipeline ingests daily flight data from Amazon S3, processes it using AWS Glue, joins it with airport dimension data, and stores the transformed data in Amazon Redshift for further analysis.

The workflow uses Amazon S3, AWS Glue Crawlers, AWS Glue ETL, Amazon EventBridge, AWS Step Functions, Amazon SNS, and Amazon Redshift.

---

## Architectural Diagram

![AirlineProject](Images/AirlineProject.jpg)

---

## Key Steps

### 1. Create an Amazon S3 Bucket

Create an S3 bucket named `airline-dataset-gds77` to store the airport dimension data and daily flight input files.

![S3](Images/S3.JPG.jpeg)

---

### 2. Create a Schema and Tables in Amazon Redshift

Create a schema named `airlines` in Amazon Redshift and create the following tables:

- `airports_dim`
- `daily_flights_fact`

Load the airport dimension data from Amazon S3 into the `airports_dim` table.

![Redshift](Images/Redshift.JPG.jpeg)

---

### 3. Create AWS Glue Crawlers for Redshift Tables

Create AWS Glue Crawlers to catalog the Redshift tables and make their metadata available for the data integration workflow.

![RedShift_Crawlers](Images/RedShift_Crawlers.JPG)

---

### 4. Create an AWS Glue Crawler for S3 Input Data

Create a Hive-style folder structure in the S3 bucket and upload a sample data file to establish the crawler's source structure.

**S3 Input Data**

![S3-crawler](Images/S3-crawler.JPG)

**S3 Flight Data**

![S3-flight-data](Images/S3-flight-data.JPG)

**AWS Glue Tables**

![glue-tables](Images/glue-tables.JPG.jpeg)

---

### 5. Create the AWS Glue ETL Job

Create an AWS Glue ETL job named `airline_data_Ingestion`.

The job processes the daily flight data, joins it with the airport dimension data, and generates a denormalized dataset for further analysis.

![glue-pipeline](Images/glue-pipeline.JPG)

---

### 6. Configure Amazon SNS Notifications

Create an Amazon SNS topic and configure an email subscription to receive workflow notifications.

![SNS](Images/SNS.JPG.jpeg)

---

### 7. Create an AWS Step Functions State Machine

Create an AWS Step Functions state machine to orchestrate the data ingestion workflow.

The state machine coordinates the required AWS services and manages the execution of the workflow.

![StateMachine](Images/StateMachine.JPG.jpeg)

---

### 8. Configure an Amazon EventBridge Rule

Configure an EventBridge rule to detect object creation events in the S3 bucket and trigger the Step Functions state machine.

![EventRule](Images/EventRule.JPG.jpeg)

---

### 9. Upload the Daily Flight Data

Upload the daily flight CSV file to the S3 input location.

The S3 object creation event triggers the EventBridge rule, which starts the Step Functions workflow.

**S3 Input File**

![fileUpload](Images/fileUpload.JPG.jpeg)

**AWS Glue Job Execution**

![glueJobRun](Images/glueJobRun.JPG.jpeg)

**Step Functions Execution**

![StateMachineSuccess](Images/StateMachineSuccess.JPG.jpeg)

---

### 10. Verify the Output

After successful execution, verify the workflow notification in the subscribed email inbox.

**SNS Email Notification**

![mailNoti](Images/mailNoti.JPG.jpeg)

Verify that the processed flight data has been stored in the Amazon Redshift fact table.

**Redshift Output Data**

![factData](Images/factData.JPG.jpeg)

---

## Final Result

Successfully implemented an event-driven airline data ingestion workflow using AWS services, from S3 data ingestion and automated orchestration to ETL processing and storage in Amazon Redshift.
