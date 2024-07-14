from utility.general_utility import (
    read_schema, fetch_file_path, flatten, read_config, fetch_transformation_query_path)



def read_file(type,
              file_name,
              spark,
              row,
              schema='NOT APPL',
              multiline=True,
              ):
    try:
        path = fetch_file_path(file_name)
        type = type.lower()
        if type == 'csv':
            if schema != 'NOT APPL':

                schema_json = read_schema(schema)
                print(schema_json)
                print(path)
                df = spark.read.schema(schema_json).option("header", True).option("delimiter", ",").csv(path)
            else:
                df = (spark.read.option("inferSchema", True).
                      option("header", True).option("delimiter", ",").csv(path))
        elif type == 'json':
            if multiline == True:
                df = spark.read.option("multiline", True).json(path)
                df = flatten(df)
            else:
                df = spark.read.option("multiple", False).json(path)
                df = flatten(df)
        elif type == 'parquet':
            df = spark.read.parquet(path)
        elif type == 'avro':
            df = spark.read.format('avro').load("path")
        elif type == 'text':
            df = spark.read.format("text").load(path)
        elif type == 'orc':
            pass
        elif type == 'xyz':
            pass
        else:
            raise ValueError("Unsupported file format", type)
        exclude_cols = row['exclude_columns'].split(',')
        return df.drop(*exclude_cols)
    except FileNotFoundError as e:
        df = None

    except Exception as e:
        df = None

def read_db(spark,
            table: str,
            database: str,
            query_path: str,
            row):
    try:
        config_data = read_config(database)
        if query_path != 'NOT APPL':
            sql_query = fetch_transformation_query_path(query_path)
            print(sql_query)
            print(config_data)
            df = spark.read.format("jdbc"). \
                option("url", config_data['url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("query", sql_query). \
                option("driver", config_data['driver']).load()
        else:
            df = spark.read.format("jdbc"). \
                option("url", config_data['jdbc_url']). \
                option("user", config_data['user']). \
                option("password", config_data['password']). \
                option("dbtable", table). \
                option("driver", config_data['driver']).load()
        exclude_cols = row['exclude_columns'].split(',')#--> ['batch_date','create_date','update_date','create_user','update_user']
        df = df.drop(*exclude_cols)

        # return df.drop('batch_date','create_date','update_date','create_user','update_user')
        return df

    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")
        return None
    except KeyError as e:
        print(f"Key error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def read_snowflake(spark,
            table: str,
            database: str,
            query: str, row):
    try:
        config_data = read_config(database)

        if query != 'NOT APPL':
            sql_query = fetch_transformation_query_path(query)
            print(sql_query)
            print(config_data)
            df = spark.read \
                .format("jdbc") \
                .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
                .option("url", config_data['jdbc_url']) \
                .option("query", sql_query) \
                .load()
        else:
            df = spark.read \
                .format("jdbc") \
                .option("driver", "net.snowflake.client.jdbc.SnowflakeDriver") \
                .option("url", config_data['jdbc_url']) \
                .option("dbtable", table) \
                .load()

        exclude_cols = row['exclude_columns'].split(',')
        return df.drop(*exclude_cols)
    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")
        return None
    except KeyError as e:
        print(f"Key error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None