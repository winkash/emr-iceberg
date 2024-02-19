import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DeltaTableManager:
    """
    A class to manage Delta Lake tables using PySpark, with integration into AWS Glue Catalog.
    """

    def __init__(self, database_name, table_name, warehouse_path, data_file):
        """
        Initializes the DeltaTableManager with necessary configurations.
        """
        logger.info("Initializing DeltaTableManager")
        self.database_name = database_name
        self.table_name = table_name
        self.warehouse_path = warehouse_path
        self.data_file = data_file
        self.spark = self._create_spark_session()

    def _create_spark_session(self):
        """
        Creates and returns a SparkSession with the necessary configurations for Delta Lake and AWS Glue Catalog.
        """
        logger.info("Creating Spark session")
        spark_session = (
            SparkSession.builder
            .appName("DeltaTableManager")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
            .enableHiveSupport()  # This is crucial for integration with AWS Glue Catalog
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

    def setup_database(self):
        """
        Creates a new database if not already present, ensuring it's registered in the AWS Glue Catalog.
        """
        logger.info("Setting up database")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name} LOCATION '{self.warehouse_path}'")

    def write_data(self, df):
        """
        Writes the DataFrame to the specified Delta table and registers it in the AWS Glue Catalog.
        """
        full_table_path = f"{self.warehouse_path}/{self.table_name}"
        logger.info(f"Writing data to Delta table at {full_table_path}")
        df.write.format("delta").mode("overwrite").save(full_table_path)
        
        # Register the Delta table in the AWS Glue Catalog
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta_poc.table1
        USING DELTA
        LOCATION 's3://upwork-usw2-transient-lfs-emr/delta_poc_v1/table1'
        """)

    def append_data(self, df):
        """
        Appends the DataFrame to the existing Delta table and ensures it's registered in the AWS Glue Catalog.
        """
        full_table_path = f"{self.warehouse_path}/{self.table_name}"
        logger.info(f"Appending data to Delta table at {full_table_path}")
        df.write.format("delta").mode("append").save(full_table_path)
        
        # Ensure the table is registered in the AWS Glue Catalog (if not already)
        if not self.spark._jsparkSession.catalog().tableExists(f"{self.database_name}.{self.table_name}"):
            self.spark.sql(f"""
            CREATE TABLE {self.database_name}.{self.table_name}
            USING DELTA
            LOCATION '{full_table_path}'
            """)

    def show_table(self):
        """
        Displays the contents of the Delta table.
        """
        full_table_path = f"{self.warehouse_path}/{self.table_name}"
        logger.info(f"Displaying contents of Delta table at {full_table_path}")
        df = self.spark.read.format("delta").load(full_table_path)
        df.show()


if __name__ == "__main__":
    logger.info("Starting Delta table management script")

    # Configuration parameters
    database_name = "delta_poc"
    table_name = "table1"
    warehouse_path = "s3://upwork-usw2-transient-lfs-emr/delta_poc_v1"
    data_file = "s3://upwork-usw2-transient-lfs-emr/iceberg_poc/SHASTA_SDC_PUBLISHED.SHERLOCK.DATE_DIM_0_0_0.snappy.parquet"

    # Instantiate the manager and perform operations
    manager = DeltaTableManager(database_name, table_name, warehouse_path, data_file)
    df = manager.read_data()
    manager.setup_database()
    manager.write_data(df)
    manager.show_table()

    logger.info("Delta table management script completed successfully")
