

from pyspark.sql import SparkSession

jar_path = r'C:\Users\bbhoi\PycharmProjects\april_automation_framework\jars\snowflake-jdbc-3.14.3.jar'
# Create SparkSession
spark = SparkSession.builder.master("local") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

jdbc_url = "jdbc:snowflake://jkztfqy-ah45956.snowflakecomputing.com/?user=DATABRICKSPYSPARKBABULU&password=Maabapa@7&warehouse=COMPUTE_WH&db=SNOWFLAKEPRACTICE_DB&schema=QA_SCHEMA"

df = spark.read \
    .format("jdbc") \
    .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
    .option("url", jdbc_url) \
    .option("query", 'select * from CONTACT_INFO_TBL')     \
    .load()
# .option("dbtable", 'CONTACT_INFO')     \

df.show()