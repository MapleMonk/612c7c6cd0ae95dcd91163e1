{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE or replace table snitch_db.maplemonk.offline_master_core as With master as ( Select *, CASE WHEN cumulative_share <= 10 THEN \'10%\' WHEN cumulative_share <= 20 THEN \'20%\' WHEN cumulative_share <= 30 THEN \'30%\' WHEN cumulative_share <= 40 THEN \'40%\' WHEN cumulative_share <= 50 THEN \'50%\' WHEN cumulative_share <= 60 THEN \'60%\' WHEN cumulative_share <= 70 THEN \'70%\' WHEN cumulative_share <= 80 THEN \'80%\' WHEN cumulative_share <= 90 THEN \'90%\' ELSE \'100%\' END AS percentage_category from ( Select *, sum(share) over (order by share desc rows between unbounded preceding and current row) cumulative_share from ( Select *,100*div0(TOTAL_SALES,sum(TOTAL_SALES) over (partition by 1)) share FROM ( Select SKU_GROUP_Final , sum(total_sales) as TOTAL_SALES from snitch_db.maplemonk.offline_master group by 1) order by share desc) order by share desc) ), Product_details AS ( SELECT * FROM ( SELECT sku_group, price, product_name, category, final_ros, natural_ros, sku_class, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY 1) AS rn FROM snitch_db.maplemonk.availability_master_v2 ) AS ranked_products WHERE rn = 1 ) SELECT a.*, b.price, b.product_name, b.category, b.final_ros, b.natural_ros, b.sku_class FROM master a LEFT JOIN Product_details b ON a.SKU_GROUP_Final = b.sku_group;",
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
                        