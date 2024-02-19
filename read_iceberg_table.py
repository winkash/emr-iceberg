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

    def __init__(self, catalog_name, database_name, table_name):
        """
        Initializes the IcebergTableManager with necessary configurations.
        """
        logger.info("Initializing IcebergTableManager")
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.table_name = table_name
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """
        Creates and returns a SparkSession with the necessary configurations for Iceberg.
        """
        logger.info("Creating Spark session")
        spark_session = (
            SparkSession.builder
            .config(f"spark.sql.catalog.{self.catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{self.catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate()
        )
        logger.info("Spark session created successfully")
        return spark_session

    def show_table(self):
        """
        Displays the contents of the Iceberg table.
        """
        logger.info(f"Displaying contents of {self.table_name} using table api")
        self.spark.table(f"{self.catalog_name}.{self.database_name}.{self.table_name}").show(5)

        logger.info(f"Displaying contents of {self.table_name} using sql command")
        self.spark.sql(f"select * from {self.catalog_name}.{self.database_name}.{self.table_name} limit 10").show()


if __name__ == "__main__":
    logger.info("Starting Iceberg table management script")

    # Configuration parameters
    catalog_name = "glue_catalog"
    database_name = "iceberg_poc"
    table_name = "active_clients"

    # Instantiate the manager and perform operations
    manager = IcebergTableManager(catalog_name, database_name, table_name)

    manager.show_table()

    logger.info("Iceberg table management script completed successfully")
