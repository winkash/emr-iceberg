import logging
import argparse
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IcebergTableManager:
    """
    A class to manage Iceberg tables using PySpark with AWS Glue and S3 integrations.

    # Command to run this script on EMR:

    # spark-submit --deploy-mode cluster --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3
    # --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    # --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
    # --conf spark.sql.catalog.spark_catalog.type=hive
    # --conf spark.sql.catalog.spark_catalog.warehouse=s3://upwork-usw2-transient-lfs-emr/iceberg_poc/iceberg_table/
    # s3://upwork-usw2-transient-lfs-emr/streaming-poc/iceberg_operation.py
    # --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
    # --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
    """

    def __init__(self, catalog_name, database_name, table_name, warehouse_path, data_file):
        """
        Initializes the IcebergTableManager with necessary configurations.
        """
        logger.info("Initializing IcebergTableManager")
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.table_name = table_name
        self.warehouse_path = warehouse_path
        self.data_file = data_file
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """
        Creates and returns a SparkSession with the necessary configurations for Iceberg.
        """
        logger.info("Creating Spark session")
        spark_session = (
            SparkSession.builder
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.warehouse", self.warehouse_path)
            .config(f"spark.sql.catalog.{self.catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate()
        )
        logger.info("Spark session created successfully")
        return spark_session

    def read_data(self):
        """
        Reads data from the specified Parquet file.
        """
        logger.info("Reading data from Parquet file")
        return self.spark.read.parquet(self.data_file)

    def setup_database_and_table(self):
        """
        Drops the existing table if it exists, and creates a new database if not already present.
        """
        logger.info("Setting up database and table")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.catalog_name}.{self.database_name}.{self.table_name}")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")

    def create_table(self, df):
        """
        Create Iceberg table based on specified DataFrame.
        """
        logger.info(f"Create table as {self.table_name}")
        df.writeTo(f"{self.catalog_name}.{self.database_name}.{self.table_name}").create()

    def append_data(self, df):
        """
        Appends the DataFrame to the existing Iceberg table.
        """
        logger.info(f"Appending data to {self.table_name}")
        df.writeTo(f"{self.catalog_name}.{self.database_name}.{self.table_name}").append()

    def show_table(self):
        """
        Displays the contents of the Iceberg table.
        """
        logger.info(f"Displaying contents of {self.table_name}")
        self.spark.table(f"{self.catalog_name}.{self.database_name}.{self.table_name}").show()


if __name__ == "__main__":
    logger.info("Starting Iceberg table management script")

    # Create the argument parser
    parser = argparse.ArgumentParser(description="Manage Iceberg tables")
    
    # Add arguments
    parser.add_argument("--table_name", help="The name of the table")
    parser.add_argument("--data_file", help="The path to the data file")

    # Parse arguments
    args = parser.parse_args()

    # Configuration parameters
    catalog_name = "glue_catalog"
    database_name = "iceberg_poc"
    table_name = args.table_name # "sherlock_date_dim"
    warehouse_path = "s3://upwork-usw2-transient-lfs-emr/iceberg_poc/iceberg_table/"
    data_file = args.data_file # "s3://upwork-usw2-transient-lfs-emr/iceberg_poc/SHASTA_SDC_PUBLISHED.SHERLOCK.DATE_DIM_0_0_0.snappy.parquet"

    # Instantiate the manager and perform operations
    manager = IcebergTableManager(catalog_name, database_name, table_name, warehouse_path, data_file)
    df = manager.read_data()
    manager.setup_database_and_table()
    manager.create_table(df)
    manager.show_table()

    logger.info("Iceberg table management script completed successfully")
