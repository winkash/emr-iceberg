from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaTableTest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS delta_poc LOCATION 's3://upwork-usw2-transient-lfs-emr/delta_poc_v1'")

spark.sql("SELECT current_database()").show()

spark.sql("""
CREATE TABLE IF NOT EXISTS delta_poc.table1
USING DELTA
LOCATION 's3://upwork-usw2-transient-lfs-emr/delta_poc_v1/table1'
""")