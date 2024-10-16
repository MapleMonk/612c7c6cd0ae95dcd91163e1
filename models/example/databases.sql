{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table snitch_db.maplemonk.final_puv_verification as SELECT _AIRBYTE_DATA:\"BARCODE\"::string AS \"BARCODE\", _AIRBYTE_DATA:\"BILL DATE\"::string AS \"BILL DATE\", _AIRBYTE_DATA:\"BILL NO.\"::string AS \"BILL NO.\", _AIRBYTE_DATA:\"BRANCH NAME\"::string AS \"BRANCH NAME\", _AIRBYTE_DATA:\"CATEGORY\"::string AS \"CATEGORY\", _AIRBYTE_DATA:\"CD VALUE\"::string AS \"CD VALUE\", _AIRBYTE_DATA:\"CD(%)\"::string AS \"CD(%)\", _AIRBYTE_DATA:\"CGST\"::string AS \"CGST\", _AIRBYTE_DATA:\"COMPANY NAME\"::string AS \"COMPANY NAME\", _AIRBYTE_DATA:\"FIT\"::string AS \"FIT\", _AIRBYTE_DATA:\"GRN NO.\"::string AS \"GRN NO.\", _AIRBYTE_DATA:\"GROSS VALUE\"::string AS \"GROSS VALUE\", _AIRBYTE_DATA:\"HSN GROUP\"::string AS \"HSN GROUP\", _AIRBYTE_DATA:\"ITEM CODE\"::string AS \"ITEM CODE\", _AIRBYTE_DATA:\"ITEM NAME\"::string AS \"ITEM NAME\", _AIRBYTE_DATA:\"NET AMOUNT\"::string AS \"NET AMOUNT\", _AIRBYTE_DATA:\"PO ORDER DATE\"::string AS \"PO ORDER DATE\", _AIRBYTE_DATA:\"PO ORDER NO.\"::string AS \"PO ORDER NO.\", _AIRBYTE_DATA:\"PRODUCT\"::string AS \"PRODUCT\", SPLIT_PART(_AIRBYTE_DATA:\"PUR QTY\", \',\', 1)::int AS \"PUR QTY\", _AIRBYTE_DATA:\"RATE\"::string AS \"RATE\", _AIRBYTE_DATA:\"RECEIPT DATE\"::string AS \"RECEIPT DATE\", _AIRBYTE_DATA:\"ROUND AMOUNT\"::string AS \"ROUND AMOUNT\", _AIRBYTE_DATA:\"SGST/IGST\"::string AS \"SGST/IGST\", _AIRBYTE_DATA:\"SIZE\"::string AS \"SIZE\", _AIRBYTE_DATA:\"SNO.\"::string AS \"SNO.\", _AIRBYTE_DATA:\"SUPPLIER NAME\"::string AS \"SUPPLIER NAME\", _AIRBYTE_DATA:\"TAX REGION\"::string AS \"TAX REGION\", _AIRBYTE_DATA:\"TAX-2(%)\"::string AS \"TAX-2(%)\", _AIRBYTE_DATA:\"TAX-2(RS)\"::string AS \"TAX-2(RS)\", _AIRBYTE_DATA:\"TAXABLE AMOUNT\"::string AS \"TAXABLE AMOUNT\", _AIRBYTE_DATA:\"GOODS IN TRANSIT\"::string AS \"GOODS IN TRANSIT\", _AIRBYTE_DATA:\"_ab_additional_properties\"::string AS \"_ab_additional_properties\", _AIRBYTE_DATA:\"_ab_source_file_last_modified\"::string AS \"_ab_source_file_last_modified\", _AIRBYTE_DATA:\"_ab_source_file_url\"::string AS \"_ab_source_file_url\" FROM snitch_db.maplemonk._AIRBYTE_RAW_puv_verification; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.OFFLINE_OUTWARD AS With final_2 as ( With final_1 as ( With final as ( With over as (SELECT *, sales_7_days_score + sales_15_days_score + sales_30_days_score + recency_score + return_rate_score + stock_level_score+ros_level_score + size_score+PERCENTAGE_CATEGORY_score AS total_score, CASE WHEN total_score > 10 THEN \'high\' WHEN total_score >5 AND total_score <= 10 THEN \'medium\' WHEN total_score <=5 AND total_score >=3 THEN \'Low\' ELSE \'No\' END AS priority from ( Select * , CASE WHEN sales_last_7_days = 0 AND first_order_date IS NOT NULL THEN 1 ELSE 0 END AS sales_7_days_score, CASE WHEN sales_last_15_days < 5 AND first_order_date IS NOT NULL THEN 2 ELSE 0 END AS sales_15_days_score, CASE WHEN sales_last_30_days < 10 AND first_order_date IS NOT NULL THEN 3 ELSE 0 END AS sales_30_days_score, CASE WHEN days_since_last_order > AVG(days_since_last_order) OVER (PARTITION BY branch_code) AND first_order_date IS NOT NULL THEN 2 ELSE 0 END AS recency_score, CASE WHEN AVERAGE_RETURN_SINCE_FIRST_ORDER > AVG(AVERAGE_RETURN_SINCE_FIRST_ORDER) OVER (PARTITION BY branch_code) AND first_order_date IS NOT NULL THEN 2 ELSE 0 END AS return_rate_score, CASE WHEN (XS_UNITS >= 10 OR S_UNITS >= 10 OR M_UNITS >= 10 OR L_UNITS >= 10 OR XL_UNITS >= 10 OR XXL_UNITS >= 10 OR XL3_UNITS >= 10) AND CATEGORY NOT IN (\'Sunglasses\', \'Perfumes\', \'Shoes\') THEN 3 WHEN (XS_UNITS > 5 OR S_UNITS > 5 OR M_UNITS > 5 OR L_UNITS > 5 OR XL_UNITS > 5 OR XXL_UNITS > 5 OR XL3_UNITS >= 5) AND CATEGORY NOT IN (\'Sunglasses\', \'Perfumes\', \'Shoes\') THEN 2 ELSE 0 END AS stock_level_score, CASE WHEN final_ros < AVG(final_ros) OVER (PARTITION BY branch_code, category) AND first_order_date IS NOT NULL THEN 2 WHEN final_ros < PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY final_ros) OVER (PARTITION BY branch_code, category) AND first_order_date IS NOT NULL THEN 3 else 0 end as ros_level_score, case when NUM_SIZE_AVAILABLE >=1 and NUM_SIZE_AVAILABLE <=2 then 2 else 0 end as size_score, case when PERCENTAGE_CATEGORY <=40 then 0 when PERCENTAGE_CATEGORY <=60 then 1 when PERCENTAGE_CATEGORY <=100 then 2 end as PERCENTAGE_CATEGORY_score from ( Select * from ( With performance as ( Select * from ( With Overall as ( Select * From ( SELECT * FROM ( WITH OFFLINE AS ( SELECT * FROM ( WITH Master AS ( SELECT * FROM ( WITH sku_group_data AS ( SELECT sku_group, BRANCH_CODE, order_date, suborder_quantity, return_quantity FROM ( WITH ABS AS ( SELECT sku_group, BRANCH_CODE, order_date, suborder_quantity FROM snitch_db.MAPLEMONK.STORE_fact_items_offline ), ACC AS ( SELECT sku_group, BRANCH_CODE, order_date, suborder_quantity AS return_quantity FROM snitch_db.MAPLEMONK.store_returns_fact_items ) SELECT A.*, IFNULL(B.return_quantity, 0) AS return_quantity FROM ABS A FULL OUTER JOIN ACC B ON A.sku_group = B.sku_group AND A.order_date = B.order_date AND A.BRANCH_CODE = B.BRANCH_CODE ) WHERE SKU_GROUP NOT LIKE \'CB%\' ), first_order_dates AS ( SELECT sku_group, BRANCH_CODE, MIN(order_date) AS first_order_date, DATEDIFF(\'DAY\', MIN(order_date), CURRENT_DATE) AS days_since_first_order, DATEDIFF(\'DAY\', MIN(order_date), MAX(order_date)) AS total_days_sold, MAX(order_date) AS last_order_date FROM sku_group_data GROUP BY sku_group, BRANCH_CODE ), total_sales AS ( SELECT sku_group_data.sku_group, sku_group_data.BRANCH_CODE, first_order_dates.first_order_date, first_order_dates.days_since_first_order, first_order_dates.total_days_sold, first_order_dates.last_order_date, DATEDIFF(\'DAY\', MAX(order_date), CURRENT_DATE) AS days_since_last_order, SUM(return_quantity) AS total_returns, SUM(suborder_quantity) AS total_sales, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 30, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_30_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 15, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_15_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 7, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_7_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 90, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_90_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 60, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_60_days, SUM(CASE WHEN order_date <= DATEADD(\'DAY\', 180, first_order_date) THEN suborder_quantity ELSE 0 END) AS sales_first_180_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 30 THEN suborder_quantity ELSE 0 END) AS sales_last_30_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 15 THEN suborder_quantity ELSE 0 END) AS sales_last_15_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 7 THEN suborder_quantity ELSE 0 END) AS sales_last_7_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 60 THEN suborder_quantity ELSE 0 END) AS sales_last_60_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 90 THEN suborder_quantity ELSE 0 END) AS sales_last_90_days, SUM(CASE WHEN CURRENT_DATE - order_date <= 180 THEN suborder_quantity ELSE 0 END) AS sales_last_180_days, CASE WHEN first_order_dates.total_days_sold = 0 THEN SUM(suborder_quantity) ELSE SUM(suborder_quantity) / first_order_dates.total_days_sold END AS natural_ros FROM sku_group_data INNER JOIN first_order_dates ON sku_group_data.sku_group = first_order_dates.sku_group AND sku_group_data.BRANCH_CODE = first_order_dates.BRANCH_CODE GROUP BY sku_group_data.sku_group, sku_group_data.BRANCH_CODE, first_order_dates.first_order_date, first_order_dates.days_since_first_order, first_order_dates.total_days_sold, first_order_dates.last_order_date ), enhanced_total_sales AS ( SELECT total_sales.*, GREATEST( total_sales.sales_first_30_days / 30, total_sales.sales_last_30_days / 30, total_sales.natural_ros ) AS max_first_30_last_30_natural_ros, GREATEST( total_sales.sales_first_30_days / 30, total_sales.sales_last_30_days / 30, total_sales.natural_ros, total_sales.sales_first_7_days / 7 ) AS max_first_30_last_30_natural_ros_first_7_days, CASE WHEN total_sales.total_sales = 0 THEN 0 ELSE (total_sales.total_returns / total_sales.total_sales) * 100 END AS average_return_since_first_order, CASE WHEN max_first_30_last_30_natural_ros > 10 THEN max_first_30_last_30_natural_ros WHEN max_first_30_last_30_natural_ros < 10 AND max_first_30_last_30_natural_ros_first_7_days > 20 THEN max_first_30_last_30_natural_ros_first_7_days WHEN total_sales.total_sales > 800 THEN GREATEST(natural_ros, sales_last_7_days / 7) ELSE natural_ros END AS final_ros FROM total_sales ) SELECT * FROM enhanced_total_sales ORDER BY total_sales DESC ) ), inventory AS ( SELECT REVERSE(SUBSTRING(REVERSE(LOGICUSERCODE), CHARINDEX(\'-\', REVERSE(LOGICUSERCODE)) + 1)) AS sku_group, BRANCH_CODE, SUM(STOCK_QTY) AS inventory, DATE, COALESCE(SUM(CASE WHEN PACK_NAME = \'XS\' OR PACK_NAME = \'28\' THEN STOCK_QTY ELSE 0 END), 0) AS XS_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'S\' OR PACK_NAME = \'30\' THEN STOCK_QTY ELSE 0 END), 0) AS S_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'M\' OR PACK_NAME = \'32\' THEN STOCK_QTY ELSE 0 END), 0) AS M_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'L\' OR PACK_NAME = \'34\' THEN STOCK_QTY ELSE 0 END), 0) AS L_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'XL\' OR PACK_NAME = \'36\' THEN STOCK_QTY ELSE 0 END), 0) AS XL_units, COALESCE(SUM(CASE WHEN PACK_NAME IN (\'XXL\', \'XXl\', \'2XL\', \'38\') THEN STOCK_QTY ELSE 0 END), 0) AS XXL_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'3XL\' THEN STOCK_QTY ELSE 0 END), 0) AS XL3_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'4XL\' THEN STOCK_QTY ELSE 0 END), 0) AS XL4_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'5XL\' THEN STOCK_QTY ELSE 0 END), 0) AS XL5_units, COALESCE(SUM(CASE WHEN PACK_NAME = \'6XL\' THEN STOCK_QTY ELSE 0 END), 0) AS XL6_units, (CASE WHEN XS_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN S_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN M_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN L_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN XXL_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL3_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL4_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL5_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL6_units > 0 THEN 1 ELSE 0 END) AS num_size_available FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand WHERE sku_group NOT LIKE \'CB%\' AND DATE = CURRENT_DATE GROUP BY sku_group,BRANCH_CODE ,DATE ) SELECT COALESCE(q.sku_group, b.sku_group) as SKU_GROUP_Final, COALESCE(q.BRANCH_CODE, b.BRANCH_CODE) as BRANCH_CODE, COALESCE(q.TOTAL_RETURNS, 0) AS TOTAL_RETURNS, COALESCE(q.FIRST_ORDER_DATE, null) AS FIRST_ORDER_DATE, COALESCE(q.DAYS_SINCE_FIRST_ORDER, 0) AS DAYS_SINCE_FIRST_ORDER, COALESCE(q.TOTAL_SALES, 0) AS TOTAL_SALES, COALESCE(q.NATURAL_ROS, 0) AS NATURAL_ROS, COALESCE(q.AVERAGE_RETURN_SINCE_FIRST_ORDER, 0) AS AVERAGE_RETURN_SINCE_FIRST_ORDER, COALESCE(q.FINAL_ROS, 0) AS FINAL_ROS, COALESCE(q.SALES_FIRST_7_DAYS, 0) AS SALES_FIRST_7_DAYS, COALESCE(q.SALES_FIRST_15_DAYS, 0) AS SALES_FIRST_15_DAYS, COALESCE(q.SALES_FIRST_30_DAYS, 0) AS SALES_FIRST_30_DAYS, COALESCE(q.SALES_LAST_7_DAYS, 0) AS SALES_LAST_7_DAYS, COALESCE(q.SALES_LAST_15_DAYS, 0) AS SALES_LAST_15_DAYS, COALESCE(q.SALES_LAST_30_DAYS, 0) AS SALES_LAST_30_DAYS, COALESCE(q.days_since_last_order, 0) AS days_since_last_order, COALESCE(b.INVENTORY, 0) AS INVENTORY, COALESCE(b.XS_UNITS, 0) as XS_UNITS, COALESCE(b.S_UNITS, 0) as S_UNITS, COALESCE(b.M_UNITS, 0) as M_UNITS, COALESCE(b.L_UNITS, 0) as L_UNITS, COALESCE(b.XL_UNITS, 0) as XL_UNITS, COALESCE(b.XXL_UNITS, 0) as XXL_UNITS, COALESCE(b.XL3_UNITS, 0) as XL3_UNITS, COALESCE(b.XL4_UNITS, 0) as XL4_UNITS, COALESCE(b.XL5_UNITS, 0) as XL5_UNITS, COALESCE(b.XL6_UNITS, 0) as XL6_UNITS, COALESCE(b.NUM_SIZE_AVAILABLE, 0) as NUM_SIZE_AVAILABLE FROM Master q FULL OUTER JOIN Inventory b ON q.sku_group = b.sku_group AND q.BRANCH_CODE = b.BRANCH_CODE ) ), availability_master_v2 AS ( SELECT * FROM ( SELECT sku_group, price, product_name, category, final_ros, natural_ros, sku_class, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY 1) AS rn FROM snitch_db.maplemonk.availability_master_v2 ) WHERE rn = 1 ) SELECT a.*, b.price, b.product_name, b.category, b.final_ros AS online_final_ros, b.natural_ros AS online_natural_ros, b.sku_class FROM OFFLINE a LEFT JOIN availability_master_v2 b ON a.SKU_GROUP_Final = b.sku_group ) WHERE SKU_GROUP_Final NOT LIKE \'CB%\' ) ), StoreSales AS ( SELECT BRANCH_CODE, SKU_GROUP_Final, SUM(TOTAL_SALES) AS TOTAL_SALES FROM Overall GROUP BY SKU_GROUP_Final,BRANCH_CODE ), RankedSales AS ( SELECT *, 100 * DIV0(TOTAL_SALES, SUM(TOTAL_SALES) OVER (PARTITION BY BRANCH_CODE)) AS share FROM StoreSales order by share desc ), Pareot as ( Select *, SUM(share) OVER (PARTITION BY BRANCH_CODE ORDER BY share DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_share from RankedSales order by share desc ), Ratio as ( Select *, CASE WHEN cumulative_share <= 10 THEN \'10\' WHEN cumulative_share <= 20 and cumulative_share >= 10 THEN \'20\' WHEN cumulative_share <= 30 and cumulative_share >= 20 THEN \'30\' WHEN cumulative_share <= 40 and cumulative_share >= 30 THEN \'40\' WHEN cumulative_share <= 50 and cumulative_share >= 40 THEN \'50\' WHEN cumulative_share <= 60 and cumulative_share >= 50 THEN \'60\' WHEN cumulative_share <= 70 and cumulative_share >= 60 THEN \'70\' WHEN cumulative_share <= 80 and cumulative_share >= 70 THEN \'80\' WHEN cumulative_share <= 90 and cumulative_share >= 80 THEN \'90\' ELSE \'100\' END AS percentage_category from Pareot ) SELECT a.*, b.share, b.cumulative_share, b.percentage_category From Overall a left join Ratio b on a.SKU_GROUP_Final = b.SKU_GROUP_Final and a.BRANCH_CODE=b.BRANCH_CODE ) ), SALE_ORDER AS ( Select * FROM ( SELECT jit.*, sd.branch_code, sd.STATE_, sd.type, sd.status, sd.Dispatch_date, sd.order_received_date, DATEDIFF(\'DAY\', sd.Dispatch_date, COALESCE(sd.order_received_date, CURRENT_DATE())) as Days_JIT FROM ( SELECT order_name, order_date, sku_group, SUM(stock_qty) AS JIT, COALESCE(SUM(CASE WHEN size_mapped = \'XS\' THEN stock_qty ELSE 0 END), 0) AS XS_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'S\' THEN stock_qty ELSE 0 END), 0) AS S_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'M\' THEN stock_qty ELSE 0 END), 0) AS M_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'L\' THEN stock_qty ELSE 0 END), 0) AS L_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'XL\' THEN stock_qty ELSE 0 END), 0) AS XL_units_store, COALESCE(SUM(CASE WHEN size_mapped IN (\'XXL\', \'XXl\', \'2XL\') THEN stock_qty ELSE 0 END), 0) AS XXL_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'3XL\' THEN stock_qty ELSE 0 END), 0) AS XL3_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'4XL\' THEN stock_qty ELSE 0 END), 0) AS XL4_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'5XL\' THEN stock_qty ELSE 0 END), 0) AS XL5_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'6XL\' THEN stock_qty ELSE 0 END), 0) AS XL6_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'NA\' THEN stock_qty ELSE 0 END), 0) AS NA_units_store, (CASE WHEN XS_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN S_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN M_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN L_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XXL_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL3_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL4_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL5_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL6_units_store > 0 THEN 1 ELSE 0 END)+ (CASE WHEN NA_units_store > 0 THEN 1 ELSE 0 END) AS num_size_available_REPLEN FROM ( SELECT order_name, order_date, sku_group, SKU, REVERSE(SUBSTRING(REVERSE(SKU), 1, POSITION(\'-\', REVERSE(SKU)) - 1)) AS size, count (distinct SALEORDERITEMCODE)AS stock_qty, CASE WHEN size = \'28\' THEN \'XS\' WHEN size = \'30\' THEN \'S\' WHEN size = \'32\' THEN \'M\' WHEN size = \'34\' THEN \'L\' WHEN size = \'36\' THEN \'XL\' WHEN size = \'38\' THEN \'XXL\' WHEN size = \'40\' THEN \'3XL\' WHEN size = \'42\' THEN \'4XL\' WHEN size = \'44\' THEN \'5XL\' WHEN size = \'46\' THEN \'6XL\' WHEN size = \'48\' THEN \'7XL\' WHEN size = \'50\' THEN \'8XL\' WHEN size = \'\' THEN \'NA\' WHEN size IS NULL THEN \'NA\' ELSE size END AS size_mapped FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE marketplace_mapped IN (\'OWN_STORE\', \'FRANCHISE_STORE\') AND order_name IN ( SELECT DISTINCT so_number FROM snitch_db.maplemonk.INWARD_DATA WHERE STATUS IN(\'On Hold\',\'Dispatched to WH\',\'Ready to Dispatch\',\'Picking\',\'Not Started\') ) GROUP BY order_name, sku_group, SKU, order_date order by stock_qty desc ) subquery GROUP BY order_name, sku_group, order_date HAVING SUM(stock_qty) > 0 ) jit LEFT JOIN ( Select Distinct so_number , BRANCH_CODE,STATE_, type, status, TO_DATE(REPLACE(ORDER_DISPATCHED_DATE,\'-\', \'/\'), \'DD/MM/YYYY\') as Dispatch_date, TO_DATE(REPLACE(order_received_date,\'-\', \'/\'), \'DD/MM/YYYY\') as order_received_date from snitch_db.maplemonk.INWARD_DATA WHERE STATUS IN(\'On Hold\',\'Dispatched to WH\',\'Ready to Dispatch\',\'Picking\',\'Not Started\') ) sd ON jit.order_name = sd.so_number ) final_query ) Select a.*, b.JIT, b.type, b.status, b.dispatch_date, b.days_jit, B.num_size_available_REPLEN From performance a left join SALE_ORDER b on a.SKU_GROUP_FINAL = b.sku_group and a.branch_code = b.branch_code ) where JIT is null and inventory >0 ) ) ), details AS ( SELECT branch_code, marketplace_mapped, ROW_NUMBER() OVER (PARTITION BY branch_code ORDER BY order_date DESC) AS rn FROM snitch_db.MAPLEMONK.STORE_fact_items_offline ) SELECT a.*, b.marketplace_mapped FROM over a LEFT JOIN ( SELECT branch_code, marketplace_mapped FROM details WHERE rn = 1 ) b ON a.branch_code = b.branch_code), On_transit as ( Select REVERSE(SUBSTRING(REVERSE(\"EXPORT ADDITIONAL ITEM CODE\"), CHARINDEX(\'-\', REVERSE(\"EXPORT ADDITIONAL ITEM CODE\")) + 1)) AS sku_group, branch_code_priority , \"EXPORT PARTY NAME\", SUM(\"EXPORT QUANTITY\") AS QTY_JIT from ( SELECT a.*, b.branch_code_priority FROM snitch_db.maplemonk.offline_jit a left join ( SELECT DISTINCT e.\"EXPORT PARTY NAME\", s.branch_code_priority FROM snitch_db.maplemonk.offline_jit e LEFT JOIN snitch_db.maplemonk.storepriority s ON e.\"EXPORT PARTY NAME\" LIKE \'%\' || s.branch_name_priority || \'%\') b on a.\"EXPORT PARTY NAME\" =b.\"EXPORT PARTY NAME\" where \"IMPORT QUANTITY\" =\'\' or \"IMPORT QUANTITY\" is null ) GROUP BY 1,2,3 ) Select a.*, b.QTY_JIT from final a left join On_transit b on a.SKU_GROUP_FINAL =b.sku_group and a.branch_code= b. branch_code_priority where b.QTY_JIT is null), Replen_exculde as ( Select REVERSE(SUBSTRING(REVERSE(SKU_CODE), CHARINDEX(\'-\', REVERSE(SKU_CODE)) + 1)) AS sku_group, branch_code, sum(ALLOCATED_UNITS)as replen_units from snitch_db.maplemonk.store_replen_4 where pareto<=70 group by 1,2) Select a.*, b.replen_units from final_1 a left join Replen_exculde b on a.SKU_GROUP_FINAL =b.sku_group and a.branch_code= b. branch_code where b.replen_units is null) , PUV_exculde as ( Select REVERSE(SUBSTRING(REVERSE(\"ITEM CODE\"), CHARINDEX(\'-\', REVERSE(\"ITEM CODE\")) + 1)) AS sku_group, branch_code_priority , \"BRANCH NAME\", SUM(\"PUR QTY\") AS PUV_JIT from ( SELECT a.*, b.branch_code_priority FROM snitch_db.maplemonk.final_puv_verification a left join ( SELECT DISTINCT e.\"BRANCH NAME\", s.branch_code_priority FROM snitch_db.maplemonk.final_puv_verification e LEFT JOIN snitch_db.maplemonk.storepriority s ON e.\"BRANCH NAME\" LIKE \'%\' || s.branch_name_priority || \'%\') b on a.\"BRANCH NAME\" =b.\"BRANCH NAME\" where \"GOODS IN TRANSIT\" = \'True\' ) GROUP BY 1,2,3) Select a.*, b.PUV_JIT from final_2 a left join PUV_exculde b on a.SKU_GROUP_FINAL =b.sku_group and a.branch_code= b. branch_code_priority where b.PUV_JIT is null; CREATE or REPLACE TABLE SNITCH_DB.MAPLEMONK.JIT_OFFLINE_GOODS AS With final as (Select * from ( Select * from ( With overall as ( Select CASE WHEN POSITION(\'-\' IN \"ITEM CODE\") > 0 AND LEFT(\"ITEM CODE\", 2) = \'SH\' THEN SPLIT_PART(\"ITEM CODE\", \'-\', 1) WHEN POSITION(\'-\' IN \"ITEM CODE\") > 0 THEN SPLIT_PART(\"ITEM CODE\", \'-\', 1) || \'-\' || SPLIT_PART(\"ITEM CODE\", \'-\', 2) ELSE \"ITEM CODE\" END AS sku_group, \"ITEM CODE\", branch_code_priority , \"BRANCH NAME\", TO_DATE( \"RECEIPT DATE\", \'DD/MM/YYYY\' ) AS Inward_date, SUM(\"PUR QTY\") AS qty, \'PUV Verification Pending\' as Status, DATEDIFF( DAY, TO_DATE(\"RECEIPT DATE\", \'DD/MM/YYYY\'), CURRENT_DATE ) AS aging from ( SELECT a.*, b.branch_code_priority FROM snitch_db.maplemonk.final_puv_verification a left join ( SELECT DISTINCT e.\"BRANCH NAME\", s.branch_code_priority FROM snitch_db.maplemonk.final_puv_verification e LEFT JOIN snitch_db.maplemonk.storepriority s ON e.\"BRANCH NAME\" LIKE \'%\' || s.branch_name_priority || \'%\') b on a.\"BRANCH NAME\" =b.\"BRANCH NAME\" where \"GOODS IN TRANSIT\" = \'True\') GROUP BY 1,2,3,4,5 Union ALL SELECT CASE WHEN POSITION(\'-\' IN \"EXPORT ADDITIONAL ITEM CODE\") > 0 AND LEFT(\"EXPORT ADDITIONAL ITEM CODE\", 2) = \'SH\' THEN SPLIT_PART(\"EXPORT ADDITIONAL ITEM CODE\", \'-\', 1) WHEN POSITION(\'-\' IN \"EXPORT ADDITIONAL ITEM CODE\") > 0 THEN SPLIT_PART(\"EXPORT ADDITIONAL ITEM CODE\", \'-\', 1) || \'-\' || SPLIT_PART(\"EXPORT ADDITIONAL ITEM CODE\", \'-\', 2) ELSE \"EXPORT ADDITIONAL ITEM CODE\" END AS sku_group, \"EXPORT ADDITIONAL ITEM CODE\", branch_code_priority, \"EXPORT PARTY NAME\", CASE WHEN \"EXPORT DOC DATE\" LIKE \'%/%/%\' THEN TO_DATE(\"EXPORT DOC DATE\", \'DD/MM/YYYY\') WHEN \"EXPORT DOC DATE\" LIKE \'%-%-%\' THEN TO_DATE(\"EXPORT DOC DATE\", \'DD-MM-YYYY\') ELSE NULL END AS Inward_date, SUM(\"EXPORT QUANTITY\") AS QTY_JIT, \'IN TRANSIT\' AS STATUS, DATEDIFF( DAY, CASE WHEN \"EXPORT DOC DATE\" LIKE \'%/%/%\' THEN TO_DATE(\"EXPORT DOC DATE\", \'DD/MM/YYYY\') WHEN \"EXPORT DOC DATE\" LIKE \'%-%-%\' THEN TO_DATE(\"EXPORT DOC DATE\", \'DD-MM-YYYY\') ELSE NULL END, CURRENT_DATE ) AS aging FROM ( SELECT a.*, b.branch_code_priority FROM snitch_db.maplemonk.offline_jit a LEFT JOIN ( SELECT DISTINCT e.\"EXPORT PARTY NAME\" AS \"EXPORT PARTY NAME\", s.branch_code_priority FROM snitch_db.maplemonk.offline_jit e LEFT JOIN snitch_db.maplemonk.storepriority s ON REGEXP_REPLACE(e.\"EXPORT PARTY NAME\", \'-[^-]*$\', \'\') LIKE \'%\' || s.branch_name_priority || \'%\' ) b ON a.\"EXPORT PARTY NAME\" = b.\"EXPORT PARTY NAME\" WHERE \"IMPORT QUANTITY\" = \'\' OR \"IMPORT QUANTITY\" IS NULL ) subquery GROUP BY sku_group, \"EXPORT ADDITIONAL ITEM CODE\", branch_code_priority, \"EXPORT PARTY NAME\", Inward_date UNION ALL Select SKU_GROUP, SKU, BRANCH_CODE, store, ORDER_DATE, sum(qty) AS qty, status, datediff(day,order_date,current_date) as Aging From ( With over as ( Select SKU_GROUP, SKU, BRANCH_CODE, STATE_, ORDER_DATE, sum(JIT) AS qty, status, datediff(day,order_date,current_date) as Aging from ( Select * FROM ( SELECT jit.*, sd.branch_code, sd.STATE_, sd.type, sd.status, sd.Dispatch_date, sd.order_received_date, DATEDIFF(\'DAY\', sd.Dispatch_date, COALESCE(sd.order_received_date, CURRENT_DATE())) as Days_JIT FROM ( SELECT order_name, order_date, sku_group, SKU, SUM(stock_qty) AS JIT, COALESCE(SUM(CASE WHEN size_mapped = \'XS\' THEN stock_qty ELSE 0 END), 0) AS XS_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'S\' THEN stock_qty ELSE 0 END), 0) AS S_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'M\' THEN stock_qty ELSE 0 END), 0) AS M_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'L\' THEN stock_qty ELSE 0 END), 0) AS L_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'XL\' THEN stock_qty ELSE 0 END), 0) AS XL_units_store, COALESCE(SUM(CASE WHEN size_mapped IN (\'XXL\', \'XXl\', \'2XL\') THEN stock_qty ELSE 0 END), 0) AS XXL_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'3XL\' THEN stock_qty ELSE 0 END), 0) AS XL3_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'4XL\' THEN stock_qty ELSE 0 END), 0) AS XL4_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'5XL\' THEN stock_qty ELSE 0 END), 0) AS XL5_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'6XL\' THEN stock_qty ELSE 0 END), 0) AS XL6_units_store, COALESCE(SUM(CASE WHEN size_mapped = \'NA\' THEN stock_qty ELSE 0 END), 0) AS NA_units_store, (CASE WHEN XS_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN S_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN M_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN L_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XXL_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL3_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL4_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL5_units_store > 0 THEN 1 ELSE 0 END) + (CASE WHEN XL6_units_store > 0 THEN 1 ELSE 0 END)+ (CASE WHEN NA_units_store > 0 THEN 1 ELSE 0 END) AS num_size_available_REPLEN FROM ( SELECT order_name, order_date, CASE WHEN POSITION(\'-\' IN sku_group) > 0 AND LEFT(sku_group, 2) = \'SH\' THEN SPLIT_PART(sku_group, \'-\', 1) WHEN POSITION(\'-\' IN sku_group) > 0 THEN SPLIT_PART(sku_group, \'-\', 1) || \'-\' || SPLIT_PART(sku_group, \'-\', 2) ELSE sku_group END as sku_group, SKU, REVERSE(SUBSTRING(REVERSE(SKU), 1, POSITION(\'-\', REVERSE(SKU)) - 1)) AS size, count (distinct SALEORDERITEMCODE)AS stock_qty, CASE WHEN size = \'28\' THEN \'XS\' WHEN size = \'30\' THEN \'S\' WHEN size = \'32\' THEN \'M\' WHEN size = \'34\' THEN \'L\' WHEN size = \'36\' THEN \'XL\' WHEN size = \'38\' THEN \'XXL\' WHEN size = \'40\' THEN \'3XL\' WHEN size = \'42\' THEN \'4XL\' WHEN size = \'44\' THEN \'5XL\' WHEN size = \'46\' THEN \'6XL\' WHEN size = \'48\' THEN \'7XL\' WHEN size = \'50\' THEN \'8XL\' WHEN size = \'\' THEN \'NA\' WHEN size IS NULL THEN \'NA\' ELSE size END AS size_mapped FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE marketplace_mapped IN (\'OWN_STORE\', \'FRANCHISE_STORE\') AND order_name IN ( SELECT DISTINCT so_number FROM snitch_db.maplemonk.INWARD_DATA WHERE STATUS not IN(\'Cancelled\',\'DUPLICATE\',\'Dispatched to Store\',\'Returned\',\'RTS-LOGIC\',\'UNFILFILLABLE\') ) GROUP BY order_name, sku_group, SKU, order_date order by stock_qty desc ) subquery GROUP BY order_name, SKU,sku_group, order_date HAVING SUM(stock_qty) > 0 ) jit LEFT JOIN ( Select Distinct so_number , BRANCH_CODE,STATE_, type, status, TO_DATE(REPLACE(ORDER_DISPATCHED_DATE,\'-\', \'/\'), \'DD/MM/YYYY\') as Dispatch_date, TO_DATE(REPLACE(order_received_date,\'-\', \'/\'), \'DD/MM/YYYY\') as order_received_date from snitch_db.maplemonk.INWARD_DATA WHERE STATUS not IN(\'Cancelled\',\'DUPLICATE\',\'Dispatched to Store\',\'Returned\',\'RTS-LOGIC\',\'UNFILFILLABLE\') ) sd ON jit.order_name = sd.so_number ) ) group by 1,2,3,4,5,7), dets as ( Select * from snitch_db.maplemonk.storepriority ) Select a.*, b.BRANCH_NAME_PRIORITY, coalesce(b.BRANCH_NAME_PRIORITY,a.state_) as Store from over a left join dets b on a.branch_code =b.BRANCH_CODE_PRIORITY ) group by 1,2,3,5,4,7 ) , deatilas as ( select * from ( select CASE WHEN POSITION(\'-\' IN sku_group) > 0 AND LEFT(sku_group, 2) = \'SH\' THEN SPLIT_PART(sku_group, \'-\', 1) WHEN POSITION(\'-\' IN sku_group) > 0 THEN SPLIT_PART(sku_group, \'-\', 1) || \'-\' || SPLIT_PART(sku_group, \'-\', 2) ELSE sku_group END as sku_group, product_name, category, final_ros, natural_ros, sku_class , ROW_NUMBER() OVER (PARTITION BY CASE WHEN POSITION(\'-\' IN sku_group) > 0 AND LEFT(sku_group, 2) = \'SH\' THEN SPLIT_PART(sku_group, \'-\', 1) WHEN POSITION(\'-\' IN sku_group) > 0 THEN SPLIT_PART(sku_group, \'-\', 1) || \'-\' || SPLIT_PART(sku_group, \'-\', 2) ELSE sku_group END ORDER BY 1) AS rn, from snitch_db.maplemonk.availability_master_v2) where rn=1 ) SELECT a.*, b.product_name, b.category, b.final_ros AS online_final_ros, b.natural_ros AS online_natural_ros, b.sku_class FROM overall a LEFT JOIN deatilas b ON a.SKU_GROUP = b.sku_group ) where sku_group not like \'%CB%\' ) ), haa as ( Select * from ( SELECT DISTINCT branch_code, marketplace_mapped, ROW_NUMBER() OVER (PARTITION BY branch_code ORDER BY order_date DESC) AS rn FROM snitch_db.maplemonk.STORE_fact_items_offline) WHERE rn = 1 ) Select a.SKU_GROUP, a.\"ITEM CODE\", a.INWARD_DATE, a.QTY, a.STATUS, a.AGING, a.PRODUCT_NAME, a.CATEGORY, a.ONLINE_FINAL_ROS, a.ONLINE_NATURAL_ROS, a.SKU_CLASS, CASE WHEN a.\"BRANCH NAME\" = \'SNITCH - COFO - INFINITY ANDHERI -MUMBAI\' THEN 28 ELSE COALESCE(b.branch_code, a.branch_code_priority) END AS branch_code, CASE WHEN a.\"BRANCH NAME\" = \'SNITCH - COFO - INFINITY ANDHERI -MUMBAI\' THEN \'SNITCH - COFO - INFINITI ANDHERI\' ELSE COALESCE(b.marketplace_mapped, a.\"BRANCH NAME\") END AS Store from final a left join haa b on a.branch_code_priority = b.branch_code;",
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
            