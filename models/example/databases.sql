{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table vahdam_db.maplemonk.vahdam_amazon_3P_inv as select \'USA\' as Geo,DATE,ASIN,sum(ifnull(try_to_number(inventory::varchar), 0)) as FBAINV, sum(ifnull(try_to_number(\"AWD Inventory\"::varchar), 0)) as ADWINV from vahdam_db.maplemonk.amazon_inv_usa group by 1,2,3 UNion all select \'CA\' as Geo,DATE,ASIN,sum(ifnull(try_to_number(inventory::varchar), 0)) as FBAINV, 0 as ADWINV from vahdam_db.maplemonk.AMAZON_INV_CA group by 1,2,3 Union all select \'UK\' as Geo,DATE,ASIN,sum(ifnull(try_to_number(inventory::varchar), 0)) as FBAINV, 0 as ADWINV from vahdam_db.maplemonk.AMAZON_INV_UK group by 1,2,3 UNion all select \'DE\' as Geo,DATE,ASIN,sum(ifnull(try_to_number(inventory::varchar), 0)) as FBAINV, 0 as ADWINV from vahdam_db.maplemonk.AMAZON_INV_DE group by 1,2,3;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from VAHDAM_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            