{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SLEEPYCAT_DB.MAPLEMONK.FINAL_SKU_MASTER AS select * from (select * ,row_number() over (partition by MARKETPLACE_SKU order by 1) frw from ( select upper(Flipkart_Seller_SKU) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select common_sku_id skucode , \"PRODUCT TITLE\" name , category , sub_category , \"FLIPKART FSN\" Flipkart_FSN ,\"Seller FK SKU Code\" Flipkart_Seller_SKU , amazon_flex_sku_code , amazon_ss_sku_code , az_child_asin , row_number() over (partition by \"Seller FK SKU Code\" order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.sku_master) where rw = 1 and upper(Flipkart_Seller_SKU) not in (\'#N/A\', \'0\', \'NA\') union select upper(Flipkart_FSN) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select common_sku_id skucode , \"PRODUCT TITLE\" name , category , sub_category , \"FLIPKART FSN\" Flipkart_FSN , amazon_flex_sku_code , amazon_ss_sku_code , az_child_asin , row_number() over (partition by \"FLIPKART FSN\" order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.sku_master) where rw = 1 and upper(Flipkart_FSN) not in (\'#N/A\', \'0\', \'NA\') union select upper(amazon_flex_sku_code) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select common_sku_id skucode , \"PRODUCT TITLE\" name , category , sub_category , \"FLIPKART FSN\" Flipkart_FSN , amazon_flex_sku_code , amazon_ss_sku_code , az_child_asin , row_number() over (partition by amazon_flex_sku_code order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.sku_master) where rw = 1 and upper(amazon_flex_sku_code) not in (\'#N/A\', \'0\', \'NA\', \'NOT\') union select upper(amazon_ss_sku_code) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select common_sku_id skucode , \"PRODUCT TITLE\" name , category , sub_category , \"FLIPKART FSN\" Flipkart_FSN , amazon_flex_sku_code , amazon_ss_sku_code , az_child_asin , row_number() over (partition by amazon_ss_sku_code order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.sku_master) where rw = 1 and upper(amazon_ss_sku_code) not in (\'#N/A\', \'0\', \'NA\', \'NOT\') union select upper(az_child_asin) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select common_sku_id skucode , \"PRODUCT TITLE\" name , category , sub_category , \"FLIPKART FSN\" Flipkart_FSN , amazon_flex_sku_code , amazon_ss_sku_code , az_child_asin , row_number() over (partition by az_child_asin order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.sku_master) where rw = 1 and upper(az_child_asin) not in (\'#N/A\', \'0\', \'NA\', \'NOT\') union select upper(skucode) MARKETPLACE_SKU ,upper(name) NAME ,upper(category) CATEGORY ,upper(sub_category) SUB_CATEGORY ,upper(skucode) skucode from (select common_sku_id skucode , \"PRODUCT TITLE\" name , category , sub_category , \"FLIPKART FSN\" Flipkart_FSN , amazon_flex_sku_code , amazon_ss_sku_code , az_child_asin , row_number() over (partition by common_sku_id order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.sku_master) where rw = 1 and upper(skucode) not in (\'#N/A\', \'0\', \'NA\', \'NOT\') ) ) where frw = 1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLEEPYCAT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        