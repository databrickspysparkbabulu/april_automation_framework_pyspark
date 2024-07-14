from pyspark.sql.functions import (
    count,col, when,upper, isnan,lit,sha2,concat,regexp_extract)
def count_check(source, target,row,Out):
    src_cnt = source.count()
    tgt_cnt = target.count()
    if src_cnt == tgt_cnt :
        write_output(validation_Type='count_check',
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=src_cnt-tgt_cnt,
                     column=row['key_col_list'],
                     Status='Pass',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)

    else:
        write_output(validation_Type='count_check',
                     source=row['source'],
                     target=row['target'],
                     Number_of_source_Records=src_cnt,
                     Number_of_target_Records=tgt_cnt,
                     Number_of_failed_Records=src_cnt - tgt_cnt,
                     column=row['key_col_list'],
                     Status='Fail',
                     source_type=row['source_type'],
                     target_type=row['target_type'],
                     Out=Out)



def duplicate_check(target, key_column, row, Out):
    key_column = key_column.split(",")
    duplicate = target.groupBy(key_column).count().where('count>1')
    #select col1,col2, count(1) from tn group by col1,col2 having count(1)>1
    duplicate.show()
    target_count = target.count()
    failed = duplicate.count()
    if failed > 0:
        print("Duplicates present")
        write_output(validation_Type='duplicate', source='NOT APPL', target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=row['key_col_list'], Status='FAIL',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)
    else:
        print("duplicates not present")
        write_output(validation_Type='duplicate', source='NOT APPL', target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=row['key_col_list'], Status='PASS',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)

def uniqueness_check(target: str,
                     unique_col_list: list,
                     Out: dict,
                     row: dict):

    unique_col_list = unique_col_list.split(",")
    target_count = target.count()
    for col in unique_col_list:
        unique_values = target.groupBy(col).count().where('count>1')
        unique_values.show()

        failed = unique_values.count()

        if failed > 0:
            print("duplicates present on ", col)
            write_output(validation_Type='uniqueness_check', source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=failed, column=col, Status='FAIL',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)
        else:
            print("Duplicates not present", col)
            write_output(validation_Type='uniqueness_check', source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=0, column=col, Status='PASS',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)

def records_present_only_in_target(source, target, keyList: str, Out, row):
    columns = keyList
    keyList = keyList.split(",")
    tms = target.select(keyList).exceptAll(source.select(keyList))
    failed_records = tms
    failed = tms.count()
    print("records_present_only_in_target and not source :" + str(failed))
    source_count = source.count()
    target_count = target.count()
    failed_records.show()
    if failed > 0:
        write_output(validation_Type="records_present_only_in_target", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=columns, Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="records_present_only_in_target", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=columns, Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)

def records_present_only_in_source(source, target, keyList, Out, row):
    columns = keyList
    keyList = keyList.split(",")
    smt = source.select(keyList).exceptAll(target.select(keyList))
    failed_records = smt
    failed = smt.count()
    print("records_present_only_in_source not in target :" + str(failed))
    source_count = source.count()
    target_count = target.count()
    failed_records.show()
    if failed > 0:
        write_output(validation_Type="records_present_only_in_source", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed, column=columns, Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="records_present_only_in_source", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=columns, Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)

def null_value_check(target, Null_columns, Out, row):
    target_count = target.count()
    Null_columns = Null_columns.split(",")
    for column in Null_columns:
        Null_df = target.select(count(when(col(column).contains('None') |
                                           upper(col(column)).contains('NULL') |
                                           upper(col(column)).contains('NA') |
                                           (col(column) == '') |
                                           col(column).isNull() |
                                           isnan(column), column
                                           )).alias("Null_value_count"))

        Null_df.show()
        failed = Null_df.collect()[0][0]

        if failed > 0:
            write_output(validation_Type="null_value_check", source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=failed, column=column, Status='FAIL',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)
        else:
            write_output(validation_Type="null_value_check", source=row['source'], target=row['target'],
                         Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                         Number_of_failed_Records=0, column=column, Status='PASS',
                         source_type='NOT APPL',
                         target_type=row['target_type'], Out=Out)



def data_compare(source, target, keycolumn, Out, row):
    columns = keycolumn
    keycolumn = keycolumn.split(",")
    keycolumn = [i.lower() for i in keycolumn]


    columnList = source.columns
    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)


    failed_count = failed.count()
    target_count = target.count()
    source_count = source.count()
    if failed_count > 0:
        write_output(validation_Type="data_compare", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed_count, column=columns, Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="data_compare", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=columns, Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)


    if failed.count() > 0:
        #failed2 = failed.sample(0.01)

        failed2 = failed.select(keycolumn).distinct().withColumn("hash_key",sha2(concat(*[col(c) for c in keycolumn]), 256))
        source = source.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)).\
            join(failed2,["hash_key"],how='left_semi').drop('hash_key')
        target = target.withColumn("hash_key", sha2(concat(*[col(c) for c in keycolumn]), 256)). \
        join(failed2,["hash_key"],how='left_semi').drop('hash_key')

        for column in columnList:
            print(column.lower())
            if column.lower() not in keycolumn:
                keycolumn.append(column)
                temp_source = source.select(keycolumn).withColumnRenamed(column, "source_" + column)
                temp_target = target.select(keycolumn).withColumnRenamed(column, "target_" + column)
                keycolumn.remove(column)
                temp_join = temp_source.join(temp_target, keycolumn, how='full_outer')
                temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
                                                        "True").otherwise("False")).filter(
                    f"comparison == False and source_{column} is not null and target_{column} is not null").show()

def schema_check(source, target, spark, Out, row):

    source.createOrReplaceTempView("source")
    target.createOrReplaceTempView("target")
    source_schema = spark.sql("describe source")
    source_schema.createOrReplaceTempView("source_schema")
    target_schema = spark.sql("describe target")
    target_schema.createOrReplaceTempView("target_schema")

    failed = spark.sql('''select * from (select lower(a.col_name) source_col_name,lower(b.col_name) target_col_name, a.data_type as source_data_type, b.data_type as target_data_type, 
    case when a.data_type=b.data_type then "pass" else "fail" end status
    from source_schema a full join target_schema b on lower(a.col_name)=lower(b.col_name)) where status='fail' ''')
    source_count = source_schema.count()
    target_count = target_schema.count()
    failed_count = failed.count()
    failed.show()
    if failed_count > 0:
        write_output(validation_Type="schema_check", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed_count, column="NOT APP", Status='FAIL',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="schema_check", source=row['source'], target=row['target'],
                     Number_of_source_Records=source_count, Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column="NOT APP", Status='PASS',
                     source_type=row['source_type'],
                     target_type=row['target_type'], Out=Out)



def name_check(target, column, Out, row, pattern=None):
    pattern = "^[a-zA-Z]"

    # Add a new column 'is_valid' indicating if the name contains only alphabetic characters
    df = target.withColumn("is_valid", regexp_extract(col(column), pattern, 0) != "")

    target_count = target.count()
    failed = df.filter('is_valid = False ')
    failed.show()
    failed_count = failed.count()
    if failed_count > 0:
        write_output(validation_Type="name_check", source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed_count, column=column, Status='FAIL',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="name_check", source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=column, Status='PASS',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)



def column_range_check(target, column, min_value, max_value, validation, row, Out):
    print("*" * 50)
    print(f"{validation} has started")
    print("*" * 50)
    failed = target.filter(f'{column} not between {min_value} and {max_value}')
    failed.show()
    failed_count = failed.count()
    target_count = target.count()
    if failed_count > 0:
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed_count, column=column, Status='FAIL',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type=validation, source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=column, Status='PASS',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)
    print("*" * 50)
    print(f"{validation} has been completed")
    print("*" * 50)



def column_value_reference_check(target, column, expected_values, Out, row):

    expected_values = expected_values.split(",")
    failed = target.withColumn("is_present", col(column).isin(expected_values)).filter('is_present = False')
    failed.show()
    failed_count = failed.count()
    target_count = target.count()
    if failed_count > 0:
        write_output(validation_Type="column_value_reference_check", source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=failed_count, column=column, Status='FAIL',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)
    else:
        write_output(validation_Type="column_value_reference_check", source=row['source'], target=row['target'],
                     Number_of_source_Records='NOT APPL', Number_of_target_Records=target_count,
                     Number_of_failed_Records=0, column=column, Status='PASS',
                     source_type='NOT APPL',
                     target_type=row['target_type'], Out=Out)


def write_output(validation_Type, source, target, Number_of_source_Records, Number_of_target_Records,
                 Number_of_failed_Records, column, Status, source_type, target_type, Out):
    Out["Source_name"].append(source)
    Out["target_name"].append(target)
    Out["column"].append(column)
    Out["validation_Type"].append(validation_Type)
    Out["Number_of_source_Records"].append(Number_of_source_Records)
    Out["Number_of_target_Records"].append(Number_of_target_Records)
    Out["Status"].append(Status)
    Out["Number_of_failed_Records"].append(Number_of_failed_Records)
    Out["source_type"].append(source_type)
    Out["target_type"].append(target_type)