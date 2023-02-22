{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table lilgoodness_db.maplemonk.amazon_manufacturing_fact_items_lg as select \'Amazon Retail & Fresh\' Shop_name ,try_to_date(date_mmddyyyy,\'mm/dd/yyyy\') Date ,ASIN ,SKU_MAPPING.SKU MAPPING_SKU ,concat(DATE,\'-AMOID-\',ASIN) order_id ,ifnull(try_to_double(replace(\"Ordered Units\",\'₹\',\'\')),0) Ordered_Units ,ifnull(try_to_double(replace(\"Ordered Revenue\",\'₹\',\'\')),0) Ordered_Revenue ,ifnull(try_to_double(replace(\"Shipped Units\",\'₹\',\'\')),0) Shipped_Units ,ifnull(try_to_double(replace(\"Shipped Revenue\",\'₹\',\'\')),0) Shipped_Revenue ,ifnull(try_to_double(replace(\"Shipped COGS\",\'₹\',\'\')),0) Shipped_COGS ,ifnull(try_to_double(replace(\"Customer Returns\",\'₹\',\'\')),0) Customer_Returns from lilgoodness_db.maplemonk.amazon_manufacturing_retail a left join (select * from (select SKU, MARKETPLACE_SKU,MARKETPLACE_NAME , row_number() over (partition by marketplace_sku order by sku) rw from LILGOODNESS_DB.maplemonk.lg_marketplace_sku_mapping ) where rw=1 ) SKU_MAPPING on lower(a.ASIN) = lower(SKU_MAPPING.marketplace_sku); create or replace table lilgoodness_db.maplemonk.amazon_sourcing_fact_items_lg as select \'Amazon_Sourcing\' Shop_name ,try_to_date(date_mmddyyyy,\'mm/dd/yyyy\') Date ,ASIN ,SKU_MAPPING.SKU MAPPING_SKU ,concat(DATE,\'-ASOID-\',ASIN) order_id ,ifnull(try_to_double(replace(\"Shipped Units\",\'₹\',\'\')),0) Shipped_Units ,ifnull(try_to_double(replace(\"Shipped Revenue\",\'₹\',\'\')),0) Shipped_Revenue ,ifnull(try_to_double(replace(\"Shipped COGS\",\'₹\',\'\')),0) Shipped_COGS ,ifnull(try_to_double(replace(\"Customer Returns\",\'₹\',\'\')),0) Customer_Returns from lilgoodness_db.maplemonk.amazon_sourcing_retail a left join (select * from (select SKU, MARKETPLACE_SKU,MARKETPLACE_NAME , row_number() over (partition by marketplace_sku order by sku) rw from LILGOODNESS_DB.maplemonk.lg_marketplace_sku_mapping ) where rw=1 ) SKU_MAPPING on lower(a.ASIN) = lower(SKU_MAPPING.marketplace_sku);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from LILGOODNESS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        