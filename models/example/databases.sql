{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.SIZE_RATIOS AS WITH total_sales AS ( SELECT sku_group, SUM(suborder_quantity) as total_group_sales_quantity from ( SELECT REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, suborder_quantity FROM snitch_db.maplemonk.unicommerce_fact_items_snitch ) GROUP BY sku_group ), subquery AS ( SELECT sku, REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, SUM(return_quantity) as total_returns, SUM(suborder_quantity) as total_sales_quantity, CASE WHEN SUM(suborder_quantity) = 0 THEN 0 ELSE SUM(return_quantity)/SUM(suborder_quantity) * 100 END AS average_returns_since_first_order, CASE WHEN SUM(suborder_quantity) = 0 THEN 0 ELSE SUM(suborder_quantity)*(1 - SUM(return_quantity)/SUM(suborder_quantity)) END AS total_sales_net_of_return FROM snitch_db.maplemonk.unicommerce_fact_items_snitch GROUP BY sku_group,sku ), total_group_sales_net_of_return AS ( SELECT sku_group, SUM(total_sales_net_of_return) as total_group_sales_net_of_return FROM subquery GROUP BY sku_group ), ranked_contributions AS ( SELECT sku_group, contribution_percentage, DENSE_RANK() OVER (PARTITION BY sku_group ORDER BY CASE WHEN contribution_percentage > 0 THEN contribution_percentage ELSE NULL END) AS contribution_rank FROM ( SELECT subquery.*, total_sales.total_group_sales_quantity, total_group_sales_net_of_return.total_group_sales_net_of_return, CASE WHEN total_group_sales_net_of_return.total_group_sales_net_of_return = 0 THEN 0 ELSE subquery.total_sales_net_of_return / total_group_sales_net_of_return.total_group_sales_net_of_return END as contribution_percentage FROM subquery LEFT JOIN total_sales ON subquery.sku_group = total_sales.sku_group LEFT JOIN total_group_sales_net_of_return ON subquery.sku_group = total_group_sales_net_of_return.sku_group ) derived ), min_contribution AS ( SELECT sku_group, MIN(contribution_percentage) AS min_non_zero_contribution FROM ranked_contributions WHERE contribution_rank = 1 GROUP BY sku_group ) SELECT subquery.*, total_sales.total_group_sales_quantity, total_group_sales_net_of_return.total_group_sales_net_of_return, CASE WHEN total_group_sales_net_of_return.total_group_sales_net_of_return = 0 THEN 0 ELSE subquery.total_sales_net_of_return / total_group_sales_net_of_return.total_group_sales_net_of_return END as contribution_percentage, ROUND(CASE WHEN min_contribution.min_non_zero_contribution = 0 THEN 0 ELSE contribution_percentage / min_contribution.min_non_zero_contribution END,1) as contribution_ratio FROM subquery LEFT JOIN total_sales ON subquery.sku_group = total_sales.sku_group LEFT JOIN total_group_sales_net_of_return ON subquery.sku_group = total_group_sales_net_of_return.sku_group LEFT JOIN min_contribution ON subquery.sku_group = min_contribution.sku_group ORDER BY total_group_sales_net_of_return desc, subquery.sku_group, subquery.sku",
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
                        