import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set
import os
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# from utility.general_utility import read_config
#
# from utility.read_data import read_file, read_db, read_snowflake
# from utility.validation_library import count_check, duplicate_check, records_present_only_in_source, \
#     records_present_only_in_target,data_compare,schema_check,null_value_check,uniqueness_check,column_value_reference_check,column_range_check,name_check

project_path = os.getcwd()

postgre_jar = project_path + "/jars/postgresql-42.2.5.jar"

snow_jar = project_path + "/jars/snowflake-jdbc-3.14.3.jar"
#oracle_jar = project_path + "/jars/ojdbc11.jar"

jar_path = snow_jar

spark = SparkSession.builder.master("local[5]") \
    .appName("test") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

Out = {
    "validation_Type": [],
    "Source_name": [],
    "target_name": [],
    "Number_of_source_Records": [],
    "Number_of_target_Records": [],
    "Number_of_failed_Records": [],
    "column": [],
    "Status": [],
    "source_type": [],
    "target_type": []
}

print("project_path", project_path)
template_path = project_path + '\config\master_test_template.xlsx'
# print(template_path)

test_cases = pd.read_excel(template_path)

# print(test_cases)

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]

run_test_case = spark.createDataFrame(run_test_case)
# print('****')
# run_test_case.show()

validation = (run_test_case.groupBy('source', 'source_type',
                                    'source_db_name', 'source_schema_path', 'source_transformation_query_path',
                                    'target', 'target_type', 'target_db_name','target_schema_path',
                                    'target_transformation_query_path',
                                    'key_col_list', 'null_col_list','exclude_columns',
                                    'unique_col_list','dq_column','expected_values','min_val','max_val').
              agg(collect_set('validation_Type').alias('validation_Type')))

validation.show(truncate=False)

validations = validation.collect()

print(validations)

