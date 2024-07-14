import os
import json
from pyspark.sql.types import StructType

from pyspark.sql.types import ArrayType

from pyspark.sql.functions import explode_outer, col

given_path = os.path.abspath(os.path.dirname(__file__))

#print(given_path)
path = os.path.dirname(given_path)

#print(path)

def read_schema(schema_file_path):
    path = os.path.dirname(given_path) + '/schema/' +schema_file_path
    # Read the JSON configuration file
    with open(path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
    return schema


def fetch_file_path(file_name):
    path = os.path.dirname(given_path) + '/source_files/'+file_name
    return path

def read_config(database):
    parent_path = os.path.dirname(given_path) + '/config/config.json'
    # Read the JSON configuration file
    with open(parent_path) as f:
        config = json.load(f)[database]
    return config

def fetch_transformation_query_path(file_path):
    path = os.path.dirname(given_path) + '/transformation_queries/' + file_path
    print("transformation_query_path",path)
    with open(path, "r") as file:
        sql_query = file.read()

    return sql_query

#print("query", fetch_transformation_query_path('test.sql'))

def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias( k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))



        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df