# import pandas as pd
# import logging
# from pyspark.sql.functions import lit, to_timestamp_ntz, current_timestamp, col, concat, date_format, regexp_replace
#
# logging.basicConfig(
#     filename='app.log',
#     level=logging.INFO,
#     format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
# )
#
# logger = logging.getLogger(__name__)
#
# def update_goodbetterbest_table(file_name):
#     file_source_type = 'goodbetterbest'
#     file_header_table = 'files_header'
#     volume_catalog = 'uut'
#     volume_schema = 'dbo'
#     volume_name = 'vol_fileuploadutility'
#     volume_directory = 'apps/goodbetterbest'
#     table_catalog = 'uut'
#     table_schema = 'silver'
#     main_table_name = 'goodbetterbest_upload'
#     archive_table_name = 'goodbetterbest_upload_archive'
#
#     try:
#         file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_directory}/{file_name}"
#         tablename = f"{table_catalog}.{table_schema}.{main_table_name}"
#
#         csv_pddf = pd.read_csv(file_path_in_volume)
#         csv_df = spark.createDataFrame(csv_pddf)
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         # logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         spark.sql(f"""
#                 insert into {volume_catalog}.{volume_schema}.{file_header_table} (file_path, file_source_type, upload_date)
#                 values ('{file_path_in_volume}', '{file_source_type}', cast(current_timestamp() as timestamp_ntz)) """)
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         file_id = spark.sql(f"""
#                             select id from {volume_catalog}.{volume_schema}.{file_header_table}
#                             where file_path = '{file_path_in_volume}'
#                             """).collect()[0][0]
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         csv_df = csv_df.withColumn("file_id", lit(file_id)) \
#                     .withColumn("loaddt", to_timestamp_ntz(current_timestamp())) \
#                     .withColumn("updatedt", to_timestamp_ntz(current_timestamp())) \
#                     .withColumn("sku",regexp_replace( concat(col('lc'), col('part')),r'[\s\.\-\/]',"")) \
#                     .withColumn("part", col('part').cast('string')) \
#                     .withColumn("file_id", col('file_id').cast('bigint'))\
#                     .withColumn("quality", col('quality').cast('string')) \
#                     .withColumn("quality_id", col('quality_id').cast('bigint'))
#
#         csv_df.createOrReplaceTempView("csvdata")
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         archive_insert_df = spark.sql(f"""
#                                         insert into {table_catalog}.{table_schema}.{archive_table_name}
#                                             (linecode,partnumber,quality_id,quality,file_id,sku,loaddt,updatedt,date_loaded, load_ts)
#                                             select
#                                                 lc,part,quality_id,quality,file_id,sku,
#                                                 loaddt,updatedt,
#                                                 cast(date_format(cast(current_timestamp() as timestamp_ntz), "yyyyMMdd") as bigint) date_loaded,
#                                                 cast(current_timestamp() as timestamp_ntz) as load_ts
#                                             from csvdata
#                                         """).collect()
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         main_upsert_df = spark.sql(f"""
#                                     MERGE INTO {table_catalog}.{table_schema}.{main_table_name} as TARGET
#                                     USING csvdata as SOURCE
#                                     ON TARGET.sku = SOURCE.sku
#                                     WHEN MATCHED THEN UPDATE SET
#                                         TARGET.linecode = SOURCE.lc,
#                                         TARGET.partnumber = SOURCE.part,
#                                         TARGET.quality_id = SOURCE.quality_id,
#                                         TARGET.quality = SOURCE.quality,
#                                         TARGET.file_id = SOURCE.file_id,
#                                         TARGET.sku = SOURCE.sku,
#                                         TARGET.loaddt = SOURCE.loaddt,
#                                         TARGET.updatedt = SOURCE.updatedt
#                                     WHEN NOT MATCHED BY TARGET THEN INSERT(linecode,partnumber,quality_id,quality,file_id,sku,loaddt,updatedt)
#                                     VALUES(
#                                         SOURCE.lc,
#                                         SOURCE.part,
#                                         SOURCE.quality_id,
#                                         SOURCE.quality,
#                                         SOURCE.file_id,
#                                         SOURCE.sku,
#                                         SOURCE.loaddt,
#                                         SOURCE.updatedt)
#                                     """).collect
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#
#
# def update_part_term_table(file_name):
#     file_source_type = 'part_term'
#     file_header_table = 'files_header'
#     volume_catalog = 'uut'
#     volume_schema = 'dbo'
#     volume_name = 'vol_fileuploadutility'
#     volume_directory = 'apps/part_term'
#     table_catalog = 'uut'
#     table_schema = 'silver'
#     main_table_name = 'part_term_upload'
#     archive_table_name = 'part_term_upload_archive'
#
#     try:
#         file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_directory}/{file_name}"
#         tablename = f"{table_catalog}.{table_schema}.{main_table_name}"
#
#         csv_pddf = pd.read_csv(file_path_in_volume)
#         csv_df = spark.createDataFrame(csv_pddf)
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         spark.sql(f"""
#                 insert into {volume_catalog}.{volume_schema}.{file_header_table} (file_path, file_source_type, upload_date)
#                 values ('{file_path_in_volume}', '{file_source_type}', cast(current_timestamp() as timestamp_ntz)) """)
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         file_id = spark.sql(f"""
#                             select id from {volume_catalog}.{volume_schema}.{file_header_table}
#                             where file_path = '{file_path_in_volume}'
#                             """).collect()[0][0]
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         csv_df = csv_df.withColumn("file_id", lit(file_id)) \
#                     .withColumn("loaddt", to_timestamp_ntz(current_timestamp())) \
#                     .withColumn("updatedt", to_timestamp_ntz(current_timestamp())) \
#                     .withColumn("sku",regexp_replace( concat(col('lc'), col('part')),r'[\s\.\-\/]',"")) \
#                     .withColumn("file_id", col('file_id').cast('bigint'))\
#                     .withColumn("part", col('part').cast('string')) \
#                     .withColumn("lc", col('lc').cast('string')) \
#                     .withColumn("parttermid", col('parttermid').cast('bigint'))
#
#         csv_df.createOrReplaceTempView("csvdata")
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         archive_insert_df = spark.sql(f"""
#                                         insert into {table_catalog}.{table_schema}.{archive_table_name}
#                                             (
#                                                 linecode,
#                                                 partnumber,
#                                                 partid,
#                                                 file_id,
#                                                 sku,
#                                                 loaddt,
#                                                 updatedt,
#                                                 date_loaded,
#                                                 load_ts
#                                             )
#                                             select
#                                                 lc,part,parttermid,file_id,sku,
#                                                 loaddt,updatedt,
#                                                 cast(date_format(cast(current_timestamp() as timestamp_ntz), "yyyyMMdd") as bigint) date_loaded,
#                                                 cast(current_timestamp() as timestamp_ntz) as load_ts
#                                             from csvdata
#                                         """).collect()
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         main_upsert_df = spark.sql(f"""
#                                     MERGE INTO {table_catalog}.{table_schema}.{main_table_name} as TARGET
#                                     USING csvdata as SOURCE
#                                     ON TARGET.sku = SOURCE.sku
#                                     WHEN MATCHED THEN UPDATE SET
#                                         TARGET.linecode = SOURCE.lc,
#                                         TARGET.partnumber = SOURCE.part,
#                                         TARGET.partid = SOURCE.parttermid,
#                                         TARGET.file_id = SOURCE.file_id,
#                                         TARGET.sku = SOURCE.sku,
#                                         TARGET.loaddt = SOURCE.loaddt,
#                                         TARGET.updatedt = SOURCE.updatedt
#                                     WHEN NOT MATCHED BY TARGET THEN INSERT(linecode,partnumber,partid,file_id,sku,loaddt,updatedt)
#                                     VALUES(
#                                         SOURCE.lc,
#                                         SOURCE.part,
#                                         SOURCE.parttermid,
#                                         SOURCE.file_id,
#                                         SOURCE.sku,
#                                         SOURCE.loaddt,
#                                         SOURCE.updatedt)
#                                     """).collect
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#
#
#
# def update_custom_part_attributes_table(file_name):
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
#     try:
#         file_path_in_volume = f"/Volumes/{volume_catalog}/{volume_schema}/{volume_name}/{volume_directory}/{file_name}"
#         tablename = f"{table_catalog}.{table_schema}.{main_table_name}"
#
#         csv_pddf = pd.read_csv(file_path_in_volume)
#         csv_df = spark.createDataFrame(csv_pddf)
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         spark.sql(f"""
#                 insert into {volume_catalog}.{volume_schema}.{file_header_table} (file_path, file_source_type, upload_date)
#                 values ('{file_path_in_volume}', '{file_source_type}', cast(current_timestamp() as timestamp_ntz)) """)
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         file_id = spark.sql(f"""
#                             select id from {volume_catalog}.{volume_schema}.{file_header_table}
#                             where file_path = '{file_path_in_volume}'
#                             """).collect()[0][0]
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         csv_df = csv_df.withColumn("file_id", lit(file_id)) \
#                     .withColumn("loaddt", to_timestamp_ntz(current_timestamp())) \
#                     .withColumn("updatedt", to_timestamp_ntz(current_timestamp())) \
#                     .withColumn("sku",regexp_replace( concat(col('lc'), col('part')),r'[\s\.\-\/]',"")) \
#                     .withColumn("file_id", col('file_id').cast('bigint'))\
#                     .withColumn("part", col('part').cast('string')) \
#                     .withColumn("lc", col('lc').cast('string')) \
#                     .withColumn("partattributecode", col('partattributecode').cast('bigint')) \
#                     .withColumn("partattributedescription", col('partattributedescription').cast('string'))
#
#         csv_df.createOrReplaceTempView("csvdata")
#     except Exception as e:
#         logger.exception(f"Error: {e}")
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
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
#
#     try:
#         main_upsert_df = spark.sql(f"""
#                                     MERGE INTO {table_catalog}.{table_schema}.{main_table_name} as TARGET
#                                     USING csvdata as SOURCE
#                                     ON TARGET.sku = SOURCE.sku
#                                     WHEN MATCHED THEN UPDATE SET
#                                         TARGET.linecode = SOURCE.lc,
#                                         TARGET.partnumber = SOURCE.part,
#                                         TARGET.partattributecode = SOURCE.partattributecode,
#                                         TARGET.partattributedescription = SOURCE.partattributedescription,
#                                         TARGET.file_id = SOURCE.file_id,
#                                         TARGET.sku = SOURCE.sku,
#                                         TARGET.loaddt = SOURCE.loaddt,
#                                         TARGET.updatedt = SOURCE.updatedt
#                                     WHEN NOT MATCHED BY TARGET THEN INSERT(linecode,partnumber,partattributecode,partattributedescription,file_id,sku,loaddt,updatedt)
#                                     VALUES(
#                                         SOURCE.lc,
#                                         SOURCE.part,
#                                         SOURCE.partattributecode,
#                                         SOURCE.partattributedescription,
#                                         SOURCE.file_id,
#                                         SOURCE.sku,
#                                         SOURCE.loaddt,
#                                         SOURCE.updatedt)
#                                     """).collect
#     except Exception as e:
#         logger.exception(f"Error: {e}")
#         raise
