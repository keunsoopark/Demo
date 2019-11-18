from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class JDBCWriter:
    def __init__(self, *,
                 parallelism_level: int = None,
                 saving_db_table_name: str = None,
                 jdbc_connection_info: Dict
                 ):
        self.parallelism_level = str(parallelism_level)  # or sc.defaultParallelism
        self.saving_db_table_name = saving_db_table_name
        self.jdbc_url = None
        self.connection_properties = None
        self.sc = SparkSession.sparkContext

    def write(self, *,
              df: DataFrame):
        if isinstance(df, DataFrame):
            print("The number of concurrent jdbc connections is {}".format(self.parallelism_level))

            df.write.option("numPartitions", self.sc.parallelism_level).jdbc(url=self.jdbc_url,
                                                                             table=self.saving_db_table_name,
                                                                             mode="append",
                                                                             properties=self.connection_properties)

