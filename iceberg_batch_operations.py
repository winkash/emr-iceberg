import logging
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql
from pyspark.sql.functions import *


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class IcebergManager:
    """
    A class to manage Iceberg tables using PySpark with AWS Glue and S3 integrations.

    # Command to run this script on EMR:

    # spark-submit --deploy-mode cluster --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3
    # --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    # --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
    # --conf spark.sql.catalog.spark_catalog.type=hive
    # --conf spark.sql.catalog.spark_catalog.warehouse=s3://upwork-usw2-transient-lfs-emr/iceberg_poc/iceberg_table1/
    # s3://upwork-usw2-transient-lfs-emr/streaming-poc/iceberg_operation.py
    # --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
    # --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
    """

    def __init__(self, catalog_name, database_name, warehouse_path):
        """
        Initializes the IcebergTableManager with necessary configurations.
        """
        logger.info("Initializing IcebergManager")
        self.catalog_name = catalog_name
        self.warehouse_path = warehouse_path
        self.database_name = database_name
        self.spark = self._create_spark_session()
        self.FEATURES = {
            'cl_count_of_hire_v1': count('*').name('cl_count_of_hire_v1'),
            'cl_count_of_client_weakness_is_unclear_requirements_v1': sum(col('client_weakness_is_something_else')).name('cl_count_of_client_weakness_is_unclear_requirements_v1'),
            'cl_count_of_client_weakness_is_poor_communication_v1': sum(col('client_weakness_is_poor_communication')).name('cl_count_of_client_weakness_is_poor_communication_v1'),
            'cl_count_of_client_weakness_is_something_else_v1': sum(col('client_weakness_is_something_else')).name('cl_count_of_client_weakness_is_something_else_v1'),
            'cl_avg_of_client_weakness_is_unclear_requirements_v1': avg(col('client_weakness_is_something_else')).name('cl_avg_of_client_weakness_is_unclear_requirements_v1'),
            'cl_avg_of_client_weakness_is_poor_communication_v1': avg(col('client_weakness_is_poor_communication')).name('cl_avg_of_client_weakness_is_poor_communication_v1'),
            'cl_avg_of_client_weakness_is_something_else_v1': avg(col('client_weakness_is_something_else')).name('cl_avg_of_client_weakness_is_something_else_v1'),
            'cl_count_of_outcome_is_good_v1': sum(col('outcome_is_good')).name('cl_count_of_outcome_is_good_v1'),
            'cl_count_of_outcome_is_neutral_v1': sum(col('outcome_is_neutral')).name('cl_count_of_outcome_is_neutral_v1'),
            'cl_count_of_outcome_is_bad_v1': sum(col('outcome_is_bad')).name('cl_count_of_outcome_is_bad_v1'),
            'cl_avg_of_outcome_is_good_v1': avg(col('outcome_is_good')).name('cl_avg_of_outcome_is_good_v1'),
            'cl_avg_of_outcome_is_neutral_v1': avg(col('outcome_is_neutral')).name('cl_avg_of_outcome_is_neutral_v1'),
            'cl_avg_of_outcome_is_bad_v1': avg(col('outcome_is_bad')).name('cl_avg_of_outcome_is_bad_v1')
        }


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

    def generate_aggregates(self, features) -> list:
        aggs_list = []
        for feature in features:
            if feature in self.FEATURES.keys():
                aggs_list.append(self.FEATURES[feature])
        return aggs_list

    def compute(
            self,
            start_date: str,
            end_date: str,
    ) -> pyspark.sql.DataFrame:
        """Computes and returns a DataFrame

        Args:
            start_date: The start date for data computation
            end_date: The end date for data computation
            features: List of Features (optional, if empty/none will set to all available)
            catalog: databricks catalog to use (currently ignored)
            schema: databricks schema to use (currently ignored)

        Returns:
            A DataFrame containing sample data

        """
        logging.info(f'computing dataframe using start_date: {start_date} end_date: {end_date}')
        feature_list_to_use = self.FEATURES.keys()

        base_population = (
            self.spark.table(f'{self.catalog_name}.{self.database_name}.active_clients')
            .filter( (col('date') >= lit(start_date)) & (col('date') < lit(end_date)))
            .select(
                'date',
                'client_id'
            )
        )
        date_fmt = '%Y-%m-%d'
        real_start_date = datetime.strptime(start_date, date_fmt).date()
        real_end_date = datetime.strptime(end_date, date_fmt).date() - timedelta(days=1)


        agg_b_assignment_outcomes = (self.spark.table(f'{self.catalog_name}.{self.database_name}.agg_b_assignment_outcomes')
        .select(
            'client_uid',
            'employer',
            'client_recom_score_answer',
            'contractor_recom_score_answer',
            'outcome',
            'end_date'
        ).filter(col("start_date") >= lit('2014-01-01'))
        )

        assignment_outcomes_with_answers = (agg_b_assignment_outcomes
                                                .withColumn('contractor_answers',
                                                            when(col('contractor_recom_score_answer').isNotNull(),
                                                                 get_json_object(col('contractor_recom_score_answer'),
                                                                                 '$.answer'))
                                                            .otherwise(lit(None))
                                                            )
                                                )
        # these values come from this Feedbacks widget:
        # https://stash.upwork.luminatesec.com/projects/OPS/repos/obo/browse/application/modules/widgets/models/Feedbacks.php#14,18,20,26

        assignment_outcomes_with_answers = (assignment_outcomes_with_answers
                                                .withColumn('client_weakness_is_unclear_requirements',
                                                            coalesce(when(col('contractor_answers').contains('"19"'),
                                                                 lit(1)).otherwise(lit(0)), lit(0)))
                                                .withColumn('client_weakness_is_poor_communication',
                                                            coalesce(when(col('contractor_answers').contains('"20"'),
                                                                 lit(1)).otherwise(lit(0)), lit(0)))
                                                .withColumn('client_weakness_is_something_else',
                                                            coalesce(when(col('contractor_answers').contains('"21"'),
                                                                 lit(1)).otherwise(lit(0)), lit(0)))
                                                )

        # The outcome is computed in agg.b_assignments using some business logic that can be found in
        # https://stash.upwork.luminatesec.com/projects/DATA/repos/dm/browse/snowflake/odw/agg/b_assignment_outcomes.sql#557
        assignment_outcomes_with_answers = (assignment_outcomes_with_answers
                                                .withColumn('outcome_is_good',
                                                            when(col('outcome') == lit('good'), 1).otherwise(0))
                                                .withColumn('outcome_is_neutral',
                                                            when(col('outcome') == lit('neutral'), 1).otherwise(0))
                                                .withColumn('outcome_is_bad',
                                                            when(col('outcome') == lit('bad'), 1).otherwise(0))
                                                )

        assignment_outcomes_users = (assignment_outcomes_with_answers
                                     .join(other=base_population,
                                           on=(assignment_outcomes_with_answers.employer == base_population.client_id)
                                              & (assignment_outcomes_with_answers.end_date < base_population.date),
                                           how="inner")
                                     )

        aggs_list = self.generate_aggregates(feature_list_to_use)
        sum_count_outcomes = (
            assignment_outcomes_users
            .groupBy(col('client_id'),
                     col('date')
                     )
            .agg(
                *aggs_list
            )
        )

        """
        left join so that we have data for all clients, even if they don't have any feedback
        """
        assignment_outcomes_all_users = (base_population
                                     .join(other=sum_count_outcomes,
                                           on= ['client_id', 'date'],
                                           how="left"
                                     )
                                     .fillna(0)
                                     )
        return assignment_outcomes_all_users




if __name__ == "__main__":
    logger.info("Starting Iceberg table management script")

    # Configuration parameters
    catalog_name = "glue_catalog"
    database_name = "iceberg_poc"
    warehouse_path = "s3://upwork-usw2-transient-lfs-emr/iceberg_poc/iceberg_table/"
    table_name = "acl_assignments_outcomes"

    # Instantiate the manager and perform operations
    manager = IcebergManager(catalog_name, database_name, warehouse_path)
    df = manager.compute('2024-02-11', '2024-02-12')
    df.filter(col('cl_count_of_hire_v1')>0).show(10, False)
    df.writeTo(f"{catalog_name}.{database_name}.{table_name}").createOrReplace()

    logger.info("Iceberg table management script completed successfully")
