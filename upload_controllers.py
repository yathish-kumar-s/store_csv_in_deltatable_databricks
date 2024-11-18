# import pandas as pd
# from pyspark.sql.functions import lit, to_timestamp_ntz, current_timestamp, col, concat, date_format, regexp_replace
# from pyspark.sql import functions as F
# import pandas as pd
#
# from database_connector import db_connector
#
#
# def cpa_uploader(file):
#     file_source_type = 'custompartattributes'
#     file_header_table = 'files_header'
#     volume_catalog = 'uut'
#     volume_schema = 'dbo'
#     volume_name = 'vol_fileuploadutility'
#     volume_directory = 'apps/custompartattributes'
#     table_catalog = 'uut'
#     table_schema = 'silver'
#     main_table_name = 'custompartattributes_upload'
#     archive_table_name = 'custompartattributes_upload_archive'
#
#     connection = db_connector()
#     cursor = connection.cursor()
#
#     csv_df = pd.read_csv(file)
#     file_name = file.filename
#     file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_directory}/{file_name}"
#
#     ## INSERT TO FILE HEADER
#     try:
#         file_header_query = f"""
#                 insert into {volume_catalog}.{volume_schema}.{file_header_table} (file_path, file_source_type, upload_date)
#                 values ('{file_path_in_volume}', '{file_source_type}', cast(current_timestamp() as timestamp_ntz))"""
#
#         cursor.execute(file_header_query)
#
#         fileid_query = f"""
#                             select id from {volume_catalog}.{volume_schema}.{file_header_table}
#                             where file_path = '{file_path_in_volume}'
#                             """
#         file_id = cursor.execute(fileid_query).fetchone().id
#
#         # file_id = spark.sql(f"""
#         #                     select id from {volume_catalog}.{volume_schema}.{file_header_table}
#         #                     where file_path = '{file_path_in_volume}'
#         #                     """).collect()[0][0]
#     except Exception as e:
#         # dbutils.notebook.exit(f"Error: {e}")
#         raise
#
#     ## MAP THE UPLOADED DATA DF ACCORDING TO TABLE
#
#     # List of column names (excluding 'PartId', 'LC', and 'Part')
#     cols = ['PartId', 'LC', 'Part']
#     value_columns = [c for c in csv_df.columns if c not in cols]
#
#     # Create a new column 'description' by collecting column names where value is 'x'
#     try:
#         csv_df['partattributedescription'] = csv_df.apply(
#             lambda row: ''.join([col if row[col] == 'x' else '' for col in value_columns]),
#             axis=1
#         )
#
#         # Remove empty descriptions (those that have no 'x' values)
#         # csv_df = csv_df.withColumn(
#         #     'description', F.when(F.col('description') != '', F.col('description')).otherwise(None)
#         # )
#         # Drop value columns after mapping
#         csv_df = csv_df.drop(*value_columns)
#
#     except Exception as e:
#         raise
#     #
#     # ## MODIFY THE UPLOADED DATA DF
#     #
#     try:
#         csv_df = csv_df.assign(
#             file_id=file_id,
#             loaddt=pd.Timestamp.now(),
#             updatedt=pd.Timestamp.now(),
#             sku=csv_df['lc'].str.cat(csv_df['part'], sep='').str.replace(r'[\s\.\-\/]', '', regex=True),
#             partattributecode='',  # nullable integer type in Pandas
#             part=csv_df['part'].astype(str),
#             lc=csv_df['lc'].astype(str),
#             partattributedescription=csv_df['partattributedescription'].astype(str)
#         )
#
#
#         csv_df.createOrReplaceTempView("csvdata")
#     except Exception as e:
#         dbutils.notebook.exit(f"Error: {e}")
#         raise
#
#     try:
#         archive_insert_df = spark.sql(f"""
#                                         insert into {table_catalog}.{table_schema}.{archive_table_name}
#                                             (
#                                                 linecode,
#                                                 partnumber,
#                                                 partattributecode,
#                                                 partattributedescription,
#                                                 file_id,
#                                                 sku,
#                                                 loaddt,
#                                                 updatedt,
#                                                 date_loaded,
#                                                 load_ts
#                                             )
#                                             select
#                                                 lc,part,partattributecode,partattributedescription,file_id,sku,
#                                                 loaddt,updatedt,
#                                                 cast(date_format(cast(current_timestamp() as timestamp_ntz), "yyyyMMdd") as bigint) date_loaded,
#                                                 cast(current_timestamp() as timestamp_ntz) as load_ts
#                                             from csvdata
#                                         """).collect()
#     #
#     #     main_upsert_df = spark.sql(f"""
#     #                                 MERGE INTO {table_catalog}.{table_schema}.{main_table_name} as TARGET
#     #                                 USING csvdata as SOURCE
#     #                                 ON TARGET.sku = SOURCE.sku
#     #                                 WHEN MATCHED THEN UPDATE SET
#     #                                     TARGET.linecode = SOURCE.lc,
#     #                                     TARGET.partnumber = SOURCE.part,
#     #                                     TARGET.partattributecode = SOURCE.partattributecode,
#     #                                     TARGET.partattributedescription = SOURCE.partattributedescription,
#     #                                     TARGET.file_id = SOURCE.file_id,
#     #                                     TARGET.sku = SOURCE.sku,
#     #                                     TARGET.loaddt = SOURCE.loaddt,
#     #                                     TARGET.updatedt = SOURCE.updatedt
#     #                                 WHEN NOT MATCHED BY TARGET THEN INSERT(linecode,partnumber,partattributecode,partattributedescription,file_id,sku,loaddt,updatedt)
#     #                                 VALUES(
#     #                                     SOURCE.lc,
#     #                                     SOURCE.part,
#     #                                     SOURCE.partattributecode,
#     #                                     SOURCE.partattributedescription,
#     #                                     SOURCE.file_id,
#     #                                     SOURCE.sku,
#     #                                     SOURCE.loaddt,
#     #                                     SOURCE.updatedt)
#     #                                 """).collect
#     # except Exception as e:
#     #     dbutils.notebook.exit(f"Error: {e}")
#     #     raise