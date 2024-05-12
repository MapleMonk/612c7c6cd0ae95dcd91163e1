{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.offline_sales_update AS WITH SALES AS ( SELECT DATE_TRUNC(\'day\', ORDER_TIMESTAMP) AS order_date, MARKETPLACE_MAPPED, SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 13 THEN SELLING_PRICE ELSE 0 END) AS sales_before_1pm, SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 16 THEN SELLING_PRICE ELSE 0 END) AS sales_before_4pm, SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 19 THEN SELLING_PRICE ELSE 0 END) AS sales_before_7pm, SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 22 THEN SELLING_PRICE ELSE 0 END) AS sales_before_10pm, SUM(selling_price) AS clossing FROM SNITCH_DB.MAPLEMONK.STORE_fact_items_offline WHERE LOWER(ORDER_STATUS) LIKE \'%process%\' GROUP BY DATE_TRUNC(\'day\', ORDER_TIMESTAMP), MARKETPLACE_MAPPED ORDER BY order_date ), RETURNS AS ( SELECT DATE_TRUNC(\'day\', ORDER_TIMESTAMP) AS return_order_date, MARKETPLACE_MAPPED AS return_MARKETPLACE_MAPPED, COALESCE(SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 13 THEN SELLING_PRICE ELSE 0 END), 0) AS Returns_before_1pm, COALESCE(SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 16 THEN SELLING_PRICE ELSE 0 END), 0) AS Returns_before_4pm, COALESCE(SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 19 THEN SELLING_PRICE ELSE 0 END), 0) AS Returns_before_7pm, COALESCE(SUM(CASE WHEN EXTRACT(HOUR FROM ORDER_TIMESTAMP) < 22 THEN SELLING_PRICE ELSE 0 END), 0) AS Returns_before_10pm, SUM(selling_price) AS closing_return FROM SNITCH_DB.MAPLEMONK.store_returns_fact_items GROUP BY return_order_date, return_MARKETPLACE_MAPPED ORDER BY return_order_date ) SELECT A.*, B.*, sales_before_1pm - COALESCE(Returns_before_1pm, 0) AS Net_sales_before_1pm, sales_before_4pm - COALESCE(Returns_before_4pm, 0) AS Net_sales_before_4pm, sales_before_7pm - COALESCE(Returns_before_7pm, 0) AS Net_sales_before_7pm, sales_before_10pm - COALESCE(Returns_before_10pm, 0) AS Net_sales_before_10pm, clossing - COALESCE(closing_return, 0) AS Clossing_sales FROM SALES A LEFT JOIN RETURNS B ON A.order_date = B.return_order_date AND A.MARKETPLACE_MAPPED = B.return_MARKETPLACE_MAPPED; CREATE OR REPLACE TABLE snitch_db.maplemonk.Offline_Detailed_Summary AS WITH Sales AS ( SELECT DATE_TRUNC(\'Month\', ORDER_DATE::DATE) AS Month, DATE_TRUNC(\'Day\', ORDER_DATE::DATE) AS Date, CASE WHEN upper(MARKETPLACE_MAPPED) LIKE \'%JAYANAGAR%\' THEN \'SNITCH - JAYANAGAR\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%VR%\' THEN \'SNITCH VR MALL\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%VARACHHA%\' THEN \'SNITCH MBH\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%MBH\' THEN \'SNITCH MBH\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%BRIGADE%\' THEN \'SNITCH BRIGADE ROAD\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%TRION%\' THEN \'SNITCH TRION -FR\' ELSE upper(MARKETPLACE_MAPPED) END AS MARKETPLACE_MAPPED1, SUM(selling_price) AS TODAY_SALES, COUNT(DISTINCT ORDER_NAME) AS Total_Orders, SUM(SUBORDER_QUANTITY) AS QTY, DIV0((SUM(SELLING_PRICE) - SUM(TAX)), SUM(SUBORDER_QUANTITY)) AS ASP, DIV0((SUM(SELLING_PRICE) - SUM(TAX)), COUNT(DISTINCT ORDER_NAME)) AS ATV, DIV0(SUM(SUBORDER_QUANTITY), COUNT(DISTINCT ORDER_NAME)) AS UPT, DIV0(SUM(MRP * SUBORDER_QUANTITY) - SUM(SELLING_PRICE - TAX), SUM(MRP * SUBORDER_QUANTITY)) AS DISC, SUM(SUM(selling_price)) OVER (PARTITION BY MARKETPLACE_MAPPED1, DATE_TRUNC(\'month\', ORDER_DATE::DATE) ORDER BY DATE_TRUNC(\'day\', ORDER_DATE::DATE) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MTD_SALES FROM SNITCH_DB.MAPLEMONK.STORE_fact_items_offline WHERE ORDER_STATUS = \'Processed\' AND category != \'CARRY BAG\' GROUP BY 1, 2, 3 ), Targets AS ( SELECT TRY_TO_DATE(\"Date \", \'DD-MM-YYYY\') AS DATE ,CASE WHEN \"Branch \" LIKE \'%JAYANAGAR%\' THEN \'SNITCH - JAYANAGAR\' WHEN \"Branch \" LIKE \'%VR SURAT%\' THEN \'SNITCH VR MALL\' WHEN \"Branch \" LIKE \'%VARACHHA%\' THEN \'SNITCH MBH\' WHEN \"Branch \" LIKE \'%BRIGADE%\' THEN \'SNITCH BRIGADE ROAD\' WHEN \"Branch \" LIKE \'%TRION VADODARA%\' THEN \'SNITCH TRION -FR\' ELSE \"Branch \" END AS Branch ,REPLACE(\"MTD \", \',\', \'\')::INT AS MTD ,REPLACE(\"Target \", \',\', \'\')::INT AS TARGET FROM SNITCH_DB.MAPLEMONK.MTD_TARGET ), Multies AS ( SELECT DATE_TRUNC(\'Day\', ORDER_DATE::DATE) AS Date, CASE WHEN upper(MARKETPLACE_MAPPED) LIKE \'%JAYANAGAR%\' THEN \'SNITCH - JAYANAGAR\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%VR%\' THEN \'SNITCH VR MALL\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%VARACHHA%\' THEN \'SNITCH MBH\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%MBH\' THEN \'SNITCH MBH\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%BRIGADE%\' THEN \'SNITCH BRIGADE ROAD\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%TRION%\' THEN \'SNITCH TRION -FR\' ELSE upper(MARKETPLACE_MAPPED) END AS MARKETPLACE_MAPPED, COUNT(DISTINCT ORDER_NAME) AS Multies FROM ( SELECT ORDER_DATE, MARKETPLACE_MAPPED, ORDER_NAME, SUM(SHIPPING_QUANTITY) AS QTY FROM SNITCH_DB.MAPLEMONK.STORE_fact_items_offline WHERE SKU_GROUP NOT LIKE \'CB%\' GROUP BY 1, 2, 3 ) WHERE QTY >= 2 GROUP BY 1, 2 ), Returns AS ( SELECT DATE_TRUNC(\'Month\', ORDER_DATE::DATE) AS Month, DATE_TRUNC(\'Day\', ORDER_DATE::DATE) AS Date, CASE WHEN upper(MARKETPLACE_MAPPED) LIKE \'%JAYANAGAR%\' THEN \'SNITCH - JAYANAGAR\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%VR%\' THEN \'SNITCH VR MALL\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%VARACHHA%\' THEN \'SNITCH MBH\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%MBH\' THEN \'SNITCH MBH\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%BRIGADE%\' THEN \'SNITCH BRIGADE ROAD\' WHEN upper(MARKETPLACE_MAPPED) LIKE \'%TRION%\' THEN \'SNITCH TRION -FR\' ELSE upper(MARKETPLACE_MAPPED) END AS MARKETPLACE_MAPPED, SUM(selling_price) AS TODAY_Returns, SUM(SUBORDER_QUANTITY) AS Return_qty, SUM(MRP * SUBORDER_QUANTITY) AS MRP_Returns FROM SNITCH_DB.MAPLEMONK.store_returns_fact_items WHERE ORDER_STATUS = \'Processed\' AND category != \'CARRY BAG\' GROUP BY 1, 2, 3 ) SELECT DATE_TRUNC(\'Month\', a.Date::DATE) AS Month, a.Date, a.MARKETPLACE_MAPPED1 as MARKETPLACE_MAPPED, a.TODAY_SALES - COALESCE(d.TODAY_Returns, 0) AS Today_Sales, b.target, SUM(SUM(a.TODAY_SALES - COALESCE(d.TODAY_Returns, 0))) OVER (PARTITION BY a.MARKETPLACE_MAPPED1, DATE_TRUNC(\'month\', a.Date::DATE) ORDER BY DATE_TRUNC(\'day\', a.Date::DATE) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MTD_SALES, b.MTD, a.Total_Orders AS Bills, COALESCE(c.Multies, 0) AS Multies, a.QTY - COALESCE(d.Return_qty, 0) AS Qty, DIV0(SUM(a.TODAY_SALES - COALESCE(d.TODAY_Returns, 0)), a.Total_Orders) AS ATV, DIV0(SUM(a.QTY - COALESCE(d.Return_qty, 0)), a.Total_Orders) AS UPT, DIV0(SUM(a.TODAY_SALES - COALESCE(d.TODAY_Returns, 0)), SUM(a.QTY - COALESCE(d.Return_qty, 0))) AS ASP FROM Sales a LEFT JOIN Targets b ON a.Date = b.DATE AND lower(a.MARKETPLACE_MAPPED1) = lower(b.Branch) LEFT JOIN Multies c ON a.Date = c.Date AND lower(a.MARKETPLACE_MAPPED1) = lower(c.MARKETPLACE_MAPPED) LEFT JOIN Returns d ON a.Date = d.Date AND lower(a.MARKETPLACE_MAPPED1) = lower(d.MARKETPLACE_MAPPED) GROUP BY DATE_TRUNC(\'Month\', a.Date::DATE), a.Date, a.MARKETPLACE_MAPPED1, a.TODAY_SALES, d.TODAY_Returns, b.target, a.Total_Orders, c.Multies, a.QTY, d.Return_qty, b.MTD ORDER BY a.Date DESC; Create or replace Table snitch_db.maplemonk.MIS_Offline_stores AS With Sales as ( SELECT DATE_TRUNC(\'MONTH\',ORDER_DATE::DATE) AS Month, ORDER_DATE, MARKETPLACE_MAPPED, COUNT(DISTINCT ORDER_NAME) AS Total_Orders, COUNT(DISTINCT PHONE) AS TOTAL_CUSTOMERS, SUM(SUBORDER_QUANTITY) AS QTY, COUNT(DISTINCT SKU_GROUP) AS SKU_GROUP, DIV0((SUM(SELLING_PRICE) - SUM(TAX)), SUM(SUBORDER_QUANTITY)) AS ASP, DIV0((SUM(SELLING_PRICE) - SUM(TAX)), COUNT(DISTINCT ORDER_NAME)) AS AOV, DIV0((SUM(SELLING_PRICE) - SUM(TAX)), COUNT(DISTINCT PHONE)) AS AOV_Customer, SUM(MRP * SUBORDER_QUANTITY)AS MRP_SALES, SUM(SELLING_PRICE) - SUM(TAX) AS Gross_Sales_Pre_GST, sum(tax) as GST , SUM(SELLING_PRICE) AS Gross_Sales_Post_GST, SUM(discount) AS Discount, SUM(COGS_PRICE * SUBORDER_QUANTITY) AS SALES_COGS FROM SNITCH_DB.MAPLEMONK.STORE_fact_items_offline WHERE ORDER_STATUS =\'Processed\' and category !=\'CARRY BAG\' GROUP BY 1, 2,3 ) , store_size as ( select * from SNITCH_DB.MAPLEMONK.Store_Size ), returns as ( SELECT DATE_TRUNC(\'MONTH\',ORDER_DATE::DATE) AS Month, ORDER_DATE, MARKETPLACE_MAPPED, SUM(SELLING_PRICE) - SUM(TAX) AS Gross_Returns_Pre_GST, SUM(SELLING_PRICE) AS Gross_returns_Post_GST, SUM(COGS_PRICE * SUBORDER_QUANTITY) AS Returns_COGS FROM SNITCH_DB.MAPLEMONK.store_returns_fact_items GROUP BY 1, 2,3 ) select COALESCE(a.Month,b.Month) as month, COALESCE(a.ORDER_DATE,b.ORDER_DATE) as DAY, COALESCE(a.MARKETPLACE_MAPPED,b.MARKETPLACE_MAPPED) as MARKETPLACE_MAPPED, a.Total_Orders, a.TOTAL_CUSTOMERS, a.QTY, a.SKU_GROUP, a.ASP, a.AOV, a.AOV_Customer, a.MRP_SALES, a.Gross_Sales_Pre_GST, a.Gross_Sales_Post_GST, a.Discount, A.GST, a.SALES_COGS, b.Gross_Returns_Pre_GST, b.Gross_returns_Post_GST, b.Returns_COGS, c.Size from sales a FULL OUTER join returns b on a.Month =b.Month AND a.ORDER_DATE =b.ORDER_DATE and a.MARKETPLACE_MAPPED =b.MARKETPLACE_MAPPED Left Join store_size c on a.MARKETPLACE_MAPPED = c.MARKETPLACE_MAPPED;",
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
                        