import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    print("Starting the delta table creation.......")
    spark = (
        SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config(f"spark.sql.catalog.glue_catalog.warehouse", "s3://upwork-usw2-transient-lfs-emr/delta_poc/")
            .getOrCreate()
    )
    logger.info("Spark session created successfully")

    # Assuming the database already exists in Glue Catalog
    database_name = "delta_poc"
    table_name = "sherlock_date_dim"

    # Read Parquet file from S3
    df = spark.read.parquet(
        "s3://upwork-usw2-transient-lfs-emr/iceberg_poc/SHASTA_SDC_PUBLISHED.SHERLOCK.DATE_DIM_0_0_0.snappy.parquet")

    logger.info(f"{df.show()}")

    df.show(10, False)

    spark.sql(f"create schema spark_catalog.{database_name}")

    # Write DataFrame to S3 as an Delta table in the existing database
    df.write.format("delta") \
        .mode("overwrite") \
        .option("path", "s3://upwork-usw2-transient-lfs-emr/delta_poc/")\
        .saveAsTable(f"spark_catalog.{database_name}.{table_name}")

    df2 = spark.read.format('delta').load("s3://upwork-usw2-transient-lfs-emr/delta_poc/")
    logger.info(f"read from s3 path : {df2.show()}")

    df3 = spark.sql(f"select * from spark_catalog.{database_name}.{table_name}")
    logger.info(f"read from catalog: {df3.show()}")


main()
# if __name__ == "__main__":
#     main()

    
