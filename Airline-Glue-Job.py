import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import col


# --------------------------------------------------
# 1. INITIALIZE GLUE JOB
# --------------------------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# --------------------------------------------------
# 2. READ AIRPORT DIMENSION TABLE FROM REDSHIFT CATALOG
# --------------------------------------------------

airport_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="airlines",
    table_name="dev_airlines_airports_dim",
    redshift_tmp_dir="s3://airline-redshift-temp-836548370628-2026/temp_data/airline_dim/",
    additional_options={
        "aws_iam_role": "arn:aws:iam::836548370628:role/RedshiftAirlineS3Role"
    },
    transformation_ctx="airport_source"
)

airport_df = airport_dyf.toDF()

print("Airport dimension schema:")
airport_df.printSchema()


# --------------------------------------------------
# 3. READ DAILY FLIGHT DATA FROM S3
# --------------------------------------------------

flight_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="airlines",
    table_name="daily_raw",
    transformation_ctx="flight_source"
)

flight_df = flight_dyf.toDF()


# --------------------------------------------------
# 4. CHECK WHETHER NEW FLIGHT DATA EXISTS
# --------------------------------------------------

has_flight_data = flight_df.limit(1).count() > 0

if has_flight_data:

    print("New flight records found. Starting ETL processing.")

    print("Daily flight data schema:")
    flight_df.printSchema()

    # --------------------------------------------------
    # 5. PREPARE AIRPORT DIMENSION DATA
    # --------------------------------------------------

    airport_df = airport_df.select(
        col("airport_id").cast("long").alias("airport_id"),
        col("city"),
        col("state"),
        col("name")
    )


    # --------------------------------------------------
    # 6. JOIN DEPARTURE AIRPORT DETAILS
    # --------------------------------------------------

    departure_airport = airport_df.select(
        col("airport_id").alias("departure_id"),
        col("city").alias("dep_city"),
        col("state").alias("dep_state"),
        col("name").alias("dep_airport")
    )

    enriched_df = flight_df.join(
        departure_airport,
        flight_df["originairportid"].cast("long") == departure_airport["departure_id"],
        "left"
    )


    # --------------------------------------------------
    # 7. JOIN ARRIVAL AIRPORT DETAILS
    # --------------------------------------------------

    arrival_airport = airport_df.select(
        col("airport_id").alias("arrival_id"),
        col("city").alias("arr_city"),
        col("state").alias("arr_state"),
        col("name").alias("arr_airport")
    )

    enriched_df = enriched_df.join(
        arrival_airport,
        enriched_df["destairportid"].cast("long") == arrival_airport["arrival_id"],
        "left"
    )


    # --------------------------------------------------
    # 8. SELECT FINAL OUTPUT COLUMNS
    # --------------------------------------------------

    final_df = enriched_df.select(
        col("carrier"),
        col("dep_airport"),
        col("arr_airport"),
        col("dep_city"),
        col("arr_city"),
        col("dep_state"),
        col("arr_state"),
        col("depdelay").cast("long").alias("dep_delay"),
        col("arrdelay").cast("long").alias("arr_delay")
    )


    # --------------------------------------------------
    # 9. VALIDATE FINAL DATA
    # --------------------------------------------------

    print("Final enriched flight schema:")
    final_df.printSchema()

    print("Sample enriched flight records:")
    final_df.show(10, truncate=False)

    print("Total enriched flight records:", final_df.count())


    # --------------------------------------------------
    # 10. CONVERT TO DYNAMICFRAME
    # --------------------------------------------------

    final_dyf = DynamicFrame.fromDF(
        final_df,
        glueContext,
        "final_flight_data"
    )


    # --------------------------------------------------
    # 11. WRITE FINAL DATA INTO REDSHIFT
    # --------------------------------------------------

    glueContext.write_dynamic_frame.from_catalog(
        frame=final_dyf,
        database="airlines",
        table_name="dev_airlines_daily_flights_fact",
        redshift_tmp_dir="s3://airline-redshift-temp-836548370628-2026/temp_data/",
        additional_options={
            "aws_iam_role": "arn:aws:iam::836548370628:role/RedshiftAirlineS3Role"
        },
        transformation_ctx="redshift_fact_table_write"
    )

    print("Flight ETL processing completed successfully.")

else:

    print("No new flight records found.")
    print("Previously processed files were skipped by Glue Bookmarks.")
    print("Skipping ETL processing.")
    print("Existing Redshift records will remain untouched.")


# --------------------------------------------------
# 12. COMMIT GLUE JOB
# --------------------------------------------------

job.commit()

print("Airline Data Ingestion Glue Job completed successfully!")
