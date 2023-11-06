{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.inward_outward as select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group ,left(\"CREATED\",10)::date date ,case when _AB_SOURCE_FILE_URL like \'%SAPL-SR%\' then \'SAPL-SR\' when _AB_SOURCE_FILE_URL like \'%SAPL-EMIZA%\' then \'SAPL-EMIZA\' when _AB_SOURCE_FILE_URL like \'%SAPL-WH%\' then \'SAPL-WH\' end warehouse_name ,sum(\"Putaway Quantity\") quantity from snitch_db.MAPLEMONK.UNICOMMERCE_PUTAWAY_REPORT where type = \'PUTAWAY_GRN_ITEM\' group by 1,2,3;",
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
                        