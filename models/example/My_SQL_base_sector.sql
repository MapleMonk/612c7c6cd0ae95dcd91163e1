{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.my_sql_base_city ADD COLUMN IF NOT EXISTS iso VARCHAR(5); MERGE INTO eggozdb.maplemonk.my_sql_base_city AS target USING ( SELECT id, CASE WHEN state = \'Haryana\' THEN \'IN-HR\' WHEN state = \'Delhi\' THEN \'IN-DL\' WHEN state = \'Uttar Pradesh\' THEN \'IN-UP\' WHEN state = \'Karnataka\' THEN \'IN-KA\' WHEN state = \'Punjab\' THEN \'IN-PB\' WHEN state = \'Rajasthan\' THEN \'IN-RJ\' WHEN state = \'Madhya Pradesh\' THEN \'IN-MP\' WHEN state = \'Bihar\' THEN \'IN-BR\' WHEN state = \'West Bengal\' THEN \'IN-WB\' WHEN state = \'Jharkhand\' THEN \'IN-JH\' WHEN state = \'Telangana\' THEN \'IN-TS\' WHEN state = \'Tamilnadu\' THEN \'IN-TN\' WHEN state = \'Maharashtra\' THEN \'IN-MH\' ELSE NULL END AS iso FROM eggozdb.maplemonk.my_sql_base_city ) AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.iso = source.iso;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.My_SQL_base_sector
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        