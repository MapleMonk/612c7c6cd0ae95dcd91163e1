{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.forward_timestamps AS SELECT \"Sale Order Code\", \"Display Order Code\", \"Shipping Package Code\", MAX(CASE WHEN \"Old Value\" = \'NEW\' AND \"New Value\" = \'CREATED\' THEN to_timestamp(\"Time Stamp\") END) AS created_timestamp, MAX(CASE WHEN \"Old Value\" = \'CREATED\' AND \"New Value\" = \'PICKING\' THEN to_timestamp(\"Time Stamp\") END) AS picking_timestamp, MAX(CASE WHEN \"Old Value\" = \'PICKING\' AND \"New Value\" = \'PICKED\' THEN to_timestamp(\"Time Stamp\") END) AS picked_timestamp, MAX(CASE WHEN \"Old Value\" = \'PICKED\' AND \"New Value\" = \'PACKED\' THEN to_timestamp(\"Time Stamp\") END) AS packed_timestamp, MAX(CASE WHEN \"Old Value\" = \'PACKED\' AND \"New Value\" = \'READY_TO_SHIP\' THEN to_timestamp(\"Time Stamp\") END) AS rts_timestamp, MAX(CASE WHEN \"Old Value\" = \'READY_TO_SHIP\' AND \"New Value\" = \'MANIFESTED\' THEN to_timestamp(\"Time Stamp\") END) AS manifested_timestamp, MAX(CASE WHEN (\"Old Value\" = \'MANIFESTED\' OR \"Old Value\" = \'READY_TO_SHIP\') AND \"New Value\" = \'DISPATCHED\' THEN to_timestamp(\"Time Stamp\")END) AS dispatched_timestamp, MAX(CASE WHEN \"Old Value\" = \'DISPATCHED\' AND \"New Value\" = \'SHIPPED\' THEN to_timestamp(\"Time Stamp\")::date END) AS shipped_timestamp, MAX(CASE WHEN (\"Old Value\" = \'SHIPPED\' OR \"Old Value\" = \'DISPATCHED\') AND \"New Value\" = \'DELIVERED\' THEN to_timestamp(\"Time Stamp\") END) AS delivered_timestamp, \"No. of Items\", CASE WHEN DATE(\"CREATED_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\") THEN 1 ELSE 0 END AS \"created&dispatch\", CASE WHEN DATE(\"PICKED_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\") THEN 1 ELSE 0 END AS \"pick&dispatch\", CASE WHEN (DATE(\"PACKED_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\") ) OR (DATE(\"RTS_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\")) THEN 1 ELSE 0 END AS \"pack&dispatch\" FROM snitch_db.maplemonk.snitch_get_shipping_package_timeline GROUP BY \"Sale Order Code\", \"Display Order Code\", \"Shipping Package Code\", \"No. of Items\";",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        