from datetime import datetime

import pandas as pd
from database_connector import db_connector

def cpa_uploader(file, file_path_in_volume):
    file_source_type = 'custompartattributes'
    file_header_table = 'files_header'
    volume_catalog = 'uut'
    volume_schema = 'dbo'
    table_catalog = 'uut'
    table_schema = 'silver'
    main_table_name = 'custompartattributes_upload'
    archive_table_name = 'custompartattributes_upload_archive'

    # connection = db_connector()
    # cursor = connection.cursor()

    csv_df = pd.read_excel(file)
    csv_df.columns = csv_df.columns.str.lower()

    # INSERT TO FILE HEADER
    try:
        file_header_query = f"""
                insert into {volume_catalog}.{volume_schema}.{file_header_table} (file_path, file_source_type, upload_date)
                values ('{file_path_in_volume}', '{file_source_type}', cast(current_timestamp() as timestamp_ntz))"""

        fileid_query = f"""
                            select id from {volume_catalog}.{volume_schema}.{file_header_table}
                            where file_path = '{file_path_in_volume}'
                            """

        with db_connector() as connection:
            with connection.cursor() as cursor:
                cursor.execute(file_header_query)
                file_id = cursor.execute(fileid_query).fetchone().id


        cols = ['partid', 'lc', 'part', 'partattributetype', 'partattributecategory']
        value_columns = [c for c in csv_df.columns if c not in cols]

        # Create a new column 'partattributedescription' by collecting column names where value is 'x'
        csv_df['partattributedescription'] = csv_df.apply(
            lambda row: ''.join([col if row[col] == 'x' else '' for col in value_columns]),
            axis=1
        )
        # Drop value columns after mapping
        csv_df = csv_df.drop(value_columns, axis=1)


        # MODIFY THE UPLOADED DATA DF
        csv_df = csv_df.assign(
            file_id=file_id,
            loaddt=pd.Timestamp.now(),
            updatedt=pd.Timestamp.now(),
            sku=csv_df['lc'].astype(str).str.cat(csv_df['part'].astype(str), sep='').str.replace(r'[\s\.\-\/]', '', regex=True) ,
            part=csv_df['part'].astype(str),
            lc=csv_df['lc'].astype(str),
            partattributetype=csv_df['partattributetype'].astype(str),
            partattributecategory=csv_df['partattributecategory'].astype(str),
            partattributedescription=csv_df['partattributedescription'].astype(str)
        )

        values_clause = ", ".join(
            [f"({', '.join(map(repr, row))})" for row in csv_df.values]
        )

        insertion_query = f"""
                  WITH inline_table (
                    partid, lc, part, partattributetype, partattributecategory,
                    partattributedescription, file_id, loaddt, updatedt, sku) AS (
                    VALUES {values_clause}
                  )   
                  insert into {table_catalog}.{table_schema}.{archive_table_name}
                        (
                            partattributetype,
                            partattributecategory,
                            partid,
                            linecode,
                            partnumber,
                            partattributecode,
                            partattributedescription,
                            file_id,
                            sku,
                            loaddt,
                            updatedt,
                            date_loaded,
                            load_ts
                        )
                        select
                            partattributetype,
                            partattributecategory,
                            partid,lc,part,partattributecode,partattributedescription,file_id,sku,loaddt,updatedt,
                            cast(date_format(cast(current_timestamp() as timestamp_ntz), "yyyyMMdd") as bigint) as date_loaded,
                            cast(current_timestamp() as timestamp_ntz) as load_ts
                        from (
                            SELECT
                                c.partattributetype as partattributetype,
                                c.partattributecategory as partattributecategory,
                                c.partid as partid, 
                                c.lc as lc,
                                c.part as part,
                                c.partattributedescription as partattributedescription,
                                c.file_id as file_id,
                                c.sku as sku,
                                c.loaddt as loaddt,
                                c.updatedt as updatedt,
                                a.partattributeid as partattributecode
                            FROM inline_table c
                            LEFT JOIN uut.dbo.custompartattributes_header a
                                ON c.partid = a.parttermid
                                AND a.partattributename = c.partattributedescription
                        )
                  """

        # with db_connector() as connection:
        #     with connection.cursor() as cursor:
        #         cursor.execute(insertion_query)

        update_query = f"""
                    WITH inline_table (
                    partid, lc, part, partattributetype, partattributecategory,
                    partattributedescription, file_id, loaddt, updatedt, sku) AS (
                    VALUES{values_clause}
                    ) 
                    MERGE INTO {table_catalog}.{table_schema}.{main_table_name} as TARGET
                    USING (
                        SELECT
                            c.partattributetype as partattributetype,
                            c.partattributecategory as partattributecategory,                                    
                            c.partid as partid, 
                            c.lc as lc,
                            c.part as part,
                            c.partattributedescription as partattributedescription,
                            c.file_id as file_id,
                            c.sku as sku,
                            c.loaddt as loaddt,
                            c.updatedt as updatedt,
                            a.partattributeid as partattributecode
                        FROM inline_table c
                        LEFT JOIN uut.dbo.custompartattributes_header a
                            ON c.partid = a.parttermid
                            AND a.partattributename = c.partattributedescription
                        ) as SOURCE
                    ON TARGET.sku = SOURCE.sku AND (
                        TARGET.partid = SOURCE.partid
                        AND TARGET.partattributetype = SOURCE.partattributetype
                        AND TARGET.partattributecategory = SOURCE.partattributecategory
                        )
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.partattributetype = SOURCE.partattributetype,
                        TARGET.partattributecategory = SOURCE.partattributecategory,                                
                        TARGET.partid = SOURCE.partid,
                        TARGET.linecode = SOURCE.lc,
                        TARGET.partnumber = SOURCE.part,
                        TARGET.sku = SOURCE.sku,
                        TARGET.partattributecode = SOURCE.partattributecode,
                        TARGET.partattributedescription = SOURCE.partattributedescription,                                    
                        TARGET.file_id = SOURCE.file_id,
                        --TARGET.loaddt = SOURCE.loaddt,
                        TARGET.updatedt = SOURCE.updatedt
                    WHEN NOT MATCHED BY TARGET THEN INSERT(
                        partattributetype,partattributecategory,
                        partid,linecode,partnumber,partattributecode,partattributedescription,file_id,sku,loaddt,updatedt)
                    VALUES(
                        SOURCE.partattributetype,
                        SOURCE.partattributecategory,
                        SOURCE.partid,
                        SOURCE.lc,
                        SOURCE.part,
                        SOURCE.partattributecode,
                        SOURCE.partattributedescription,                                    
                        SOURCE.file_id,
                        SOURCE.sku,
                        SOURCE.loaddt,
                        SOURCE.updatedt)
                    """
        with db_connector() as connection:
            with connection.cursor() as cursor:
                cursor.execute(insertion_query)
                cursor.execute(update_query)

    except Exception as e:
        print(e)
        raise Exception(e)


def part_term_uploader(file, file_path_in_volume):
    file_source_type = 'part_term'
    file_header_table = 'files_header'
    volume_catalog = 'uut'
    volume_schema = 'dbo'
    table_catalog = 'uut'
    table_schema = 'silver'
    main_table_name = 'part_term_upload'
    archive_table_name = 'part_term_upload_archive'

    csv_df = pd.read_excel(file)
    csv_df.columns = csv_df.columns.str.lower()

    try:
        file_header_query = f"""
                        insert into {volume_catalog}.{volume_schema}.{file_header_table} (file_path, file_source_type, upload_date)
                        values ('{file_path_in_volume}', '{file_source_type}', cast(current_timestamp() as timestamp_ntz))"""

        fileid_query = f"""
                                    select id from {volume_catalog}.{volume_schema}.{file_header_table}
                                    where file_path = '{file_path_in_volume}'
                                    """

        with db_connector() as connection:
            with connection.cursor() as cursor:
                cursor.execute(file_header_query)
                file_id = cursor.execute(fileid_query).fetchone().id

        csv_df = csv_df.assign(
            file_id=file_id,
            loaddt=pd.Timestamp.now(),
            updatedt=pd.Timestamp.now(),
            sku=csv_df['lc'].astype(str).str.cat(csv_df['part'].astype(str), sep='').str.replace(r'[\s\.\-\/]', '',
                                                                                                 regex=True),
            part=csv_df['part'].astype(str),
            lc=csv_df['lc'].astype(str),
            parttermid=csv_df['parttermid'].astype(int)
        )

        values_clause = ", ".join(
            [f"({', '.join(map(repr, row))})" for row in csv_df.values]
        )

        columns_clause = ", ".join(csv_df.columns)

        insertion_query =f"""
                            with inline_table ( {columns_clause}) as  (values {values_clause})
                            insert into {table_catalog}.{table_schema}.{archive_table_name}
                                (
                                    linecode,
                                    partnumber,
                                    partid,
                                    file_id,
                                    sku,
                                    loaddt,
                                    updatedt,
                                    date_loaded,
                                    load_ts
                                )
                                select
                                    lc,part,parttermid,file_id,sku,
                                    loaddt,updatedt,
                                    cast(date_format(cast(current_timestamp() as timestamp_ntz), "yyyyMMdd") as bigint) date_loaded,
                                    cast(current_timestamp() as timestamp_ntz) as load_ts
                                from inline_table
                            """

        update_query = f"""
                        with inline_table ( {columns_clause}) as  (values {values_clause})
                        MERGE INTO {table_catalog}.{table_schema}.{main_table_name} as TARGET
                        USING inline_table as SOURCE
                        ON TARGET.sku = SOURCE.sku
                        WHEN MATCHED THEN UPDATE SET
                            TARGET.linecode = SOURCE.lc,
                            TARGET.partnumber = SOURCE.part,
                            TARGET.partid = SOURCE.parttermid,
                            TARGET.file_id = SOURCE.file_id,
                            TARGET.sku = SOURCE.sku,
                            --TARGET.loaddt = SOURCE.loaddt,
                            TARGET.updatedt = SOURCE.updatedt
                        WHEN NOT MATCHED BY TARGET THEN INSERT(linecode,partnumber,partid,file_id,sku,loaddt,updatedt)
                        VALUES(
                            SOURCE.lc,
                            SOURCE.part,
                            SOURCE.parttermid,
                            SOURCE.file_id,
                            SOURCE.sku,
                            SOURCE.loaddt,
                            SOURCE.updatedt)
                        """
        with db_connector() as connection:
            with connection.cursor() as cursor:
                insertion_result = cursor.execute(insertion_query)
                update_result = cursor.execute(update_query)

    except Exception as e:
        print(e)
        raise Exception(e)


def gbb_uploader(file, file_path_in_volume):
    file_source_type = 'goodbetterbest'
    file_header_table = 'files_header'
    volume_catalog = 'uut'
    volume_schema = 'dbo'
    table_catalog = 'uut'
    table_schema = 'silver'
    main_table_name = 'goodbetterbest_upload'
    archive_table_name = 'goodbetterbest_upload_archive'

    csv_df = pd.read_excel(file)
    csv_df.columns = csv_df.columns.str.lower()

    try:
        file_header_query = f"""
                               insert into {volume_catalog}.{volume_schema}.{file_header_table} (file_path, file_source_type, upload_date)
                               values ('{file_path_in_volume}', '{file_source_type}', cast(current_timestamp() as timestamp_ntz))"""

        fileid_query = f"""
                           select id from {volume_catalog}.{volume_schema}.{file_header_table}
                           where file_path = '{file_path_in_volume}'
                           """

        with db_connector() as connection:
            with connection.cursor() as cursor:
                cursor.execute(file_header_query)
                file_id = cursor.execute(fileid_query).fetchone().id

        cols = ['lc', 'part']
        value_columns = [c for c in csv_df.columns if c not in cols]

        csv_df['quality'] = csv_df.apply(
            lambda row: ''.join([col if row[col] == 'x' else '' for col in value_columns]),
            axis=1
        )

        # Drop value columns after mapping
        csv_df = csv_df.drop(value_columns, axis=1)

        current_time = datetime.now()

        quality_mapping = {
            "good": 1,
            "better": 2,
            "best": 3,
            "ultra_premium": 4
        }

        csv_df = csv_df.assign(
            lc=lambda df: df['lc'].astype(str),
            part=lambda df: df['part'].astype(str),
            file_id=lambda df: int(file_id),  # Ensure file_id is defined in your script
            sku=lambda df: (df['lc'] + df['part']).str.replace(r'[\s\.\-\/]', '', regex=True),
            loaddt=lambda df: current_time,
            updatedt=lambda df: current_time,
            quality=lambda df: df['quality'].astype(str),
            quality_id=lambda df: df['quality'].map(quality_mapping).astype('Int64')
        )

        values_clause = ", ".join(
            [f"({', '.join(map(repr, row))})" for row in csv_df.values]
        )
        columns_clause = ", ".join(csv_df.columns)

        insertion_query = f"""
                            with inline_table ( {columns_clause}) as  (values {values_clause})
                            INSERT INTO {table_catalog}.{table_schema}.{archive_table_name}
                                (linecode,partnumber,quality_id,quality,file_id,sku,loaddt,updatedt,date_loaded, load_ts)
                                SELECT
                                    lc,part,quality_id,quality,file_id,sku,
                                    loaddt,updatedt,
                                    cast(date_format(cast(current_timestamp() as timestamp_ntz), "yyyyMMdd") as bigint) date_loaded,
                                    loaddt as load_ts
                                    --cast(current_timestamp() as timestamp_ntz) as load_ts
                                FROM inline_table
                            """

        update_query = f"""
                    with inline_table ( {columns_clause}) as  (values {values_clause})
                    MERGE INTO {table_catalog}.{table_schema}.{main_table_name} as TARGET
                    USING inline_table as SOURCE
                    ON TARGET.sku = SOURCE.sku
                    WHEN MATCHED THEN UPDATE SET
                        TARGET.linecode = SOURCE.lc,
                        TARGET.partnumber = SOURCE.part,
                        TARGET.quality_id = SOURCE.quality_id,
                        TARGET.quality = SOURCE.quality,
                        TARGET.file_id = SOURCE.file_id,
                        TARGET.sku = SOURCE.sku,
                        --TARGET.loaddt = SOURCE.loaddt,
                        TARGET.updatedt = SOURCE.updatedt
                    WHEN NOT MATCHED BY TARGET THEN INSERT(linecode,partnumber,quality_id,quality,file_id,sku,loaddt,updatedt)
                    VALUES(
                        SOURCE.lc,
                        SOURCE.part,
                        SOURCE.quality_id,
                        SOURCE.quality,
                        SOURCE.file_id,
                        SOURCE.sku,
                        SOURCE.loaddt,
                        SOURCE.updatedt)
                    """
        with db_connector() as connection:
            with connection.cursor() as cursor:
                insertion_result = cursor.execute(insertion_query)
                update_result = cursor.execute(update_query)

    except Exception as e:
        print(e)
        raise Exception(e)