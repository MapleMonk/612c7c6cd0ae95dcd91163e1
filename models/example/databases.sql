{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table snitch_db.maplemonk.offline_pareto as With over as ( With overall as ( WITH sku_group_data AS ( SELECT BRANCH_CODE, sku_group as sku_group_final, order_date, suborder_quantity -return_quantity as QTY, SELLING_PRICE - return_sales as Total_sales, FROM ( WITH ABS AS ( SELECT sku_group, BRANCH_CODE, order_date, suborder_quantity, SELLING_PRICE, FROM snitch_db.MAPLEMONK.STORE_fact_items_offline ), ACC AS ( SELECT sku_group, BRANCH_CODE, order_date, suborder_quantity AS return_quantity, SELLING_PRICE AS Return_sales FROM snitch_db.MAPLEMONK.store_returns_fact_items ) SELECT A.*, coalesce(B.return_quantity, 0) AS return_quantity, coalesce(B.Return_sales,0) as return_sales FROM ABS A FULL OUTER JOIN ACC B ON A.sku_group = B.sku_group AND A.order_date = B.order_date AND A.BRANCH_CODE = B.BRANCH_CODE ) WHERE SKU_GROUP NOT LIKE \'CB%\' ), StoreSales AS ( SELECT BRANCH_CODE, sku_group_final, SUM(TOTAL_SALES) AS TOTAL_SALES FROM sku_group_data where CURRENT_DATE - order_date <= 30 GROUP BY sku_group_final,BRANCH_CODE ), RankedSales AS ( SELECT *, 100 * DIV0(TOTAL_SALES, SUM(TOTAL_SALES) OVER (PARTITION BY BRANCH_CODE)) AS share FROM StoreSales order by share desc ), Pareot as ( Select *, SUM(share) OVER (PARTITION BY BRANCH_CODE ORDER BY share DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_share from RankedSales order by share desc ), Ratio as ( Select *, CASE WHEN cumulative_share <= 10 THEN \'10%\' WHEN cumulative_share <= 20 and cumulative_share >= 10 THEN \'20%\' WHEN cumulative_share <= 30 and cumulative_share >= 20 THEN \'30%\' WHEN cumulative_share <= 40 and cumulative_share >= 30 THEN \'40%\' WHEN cumulative_share <= 50 and cumulative_share >= 40 THEN \'50%\' WHEN cumulative_share <= 60 and cumulative_share >= 50 THEN \'60%\' WHEN cumulative_share <= 70 and cumulative_share >= 60 THEN \'70%\' WHEN cumulative_share <= 80 and cumulative_share >= 70 THEN \'80%\' WHEN cumulative_share <= 90 and cumulative_share >= 80 THEN \'90%\' ELSE \'100%\' END AS percentage_category from Pareot) Select * from Ratio ), details as ( Select * from ( SELECT branch_code, marketplace_mapped, ROW_NUMBER() OVER (PARTITION BY branch_code ORDER BY order_date DESC) AS row_number FROM snitch_db.maplemonk.STORE_fact_items_offline) where row_number =1 ) Select a.*, b.marketplace_mapped, from overall a left join details b on a.branch_code = b.branch_code ), class as ( Select sku_group, category, sku_class, product_name from snitch_db.maplemonk.availability_master_v2 ) Select a.*, b.category, b.sku_class, b.product_name from over a left join class b on a.SKU_GROUP_FINAL = b.sku_group ;",
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
            