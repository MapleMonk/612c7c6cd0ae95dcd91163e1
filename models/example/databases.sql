{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table FUAARK_DB.MAPLEMONK.FUAARK_DB_inventory_fact_items as with Inventory_Data_Base as ( select * from (select * ,row_number() over (partition by replace(SKU,\'\\'\',\'\'), DATA_FETCH_DATE order by _airbyte_normalized_at desc) rw from (select replace(SKU,\' \',\'\') SKU ,_airbyte_normalized_at::date DATA_FETCH_DATE ,TRY_TO_DOUBLE(QUANTITY) AVAILABLEINVENTORY ,\'BOTTOMS\' GS_CATEGORY ,_airbyte_normalized_at from FUAARK_DB.MAPLEMONK.GS_INVENTORY_BOTTOMS union all select replace(\" SKU\",\' \',\'\') SKU ,_airbyte_normalized_at::date DATA_FETCH_DATE ,TRY_TO_DOUBLE(QUANTITY) AVAILABLEINVENTORY ,\'CAPS\' GS_CATEGORY ,_airbyte_normalized_at from FUAARK_DB.MAPLEMONK.GS_INVENTORY_CAPS union all select replace(\" SKU\",\' \',\'\') SKU ,_airbyte_normalized_at::date DATA_FETCH_DATE ,TRY_TO_DOUBLE(QUANTITY) AVAILABLEINVENTORY ,\'INNERWEAR\' GS_CATEGORY ,_airbyte_normalized_at from FUAARK_DB.MAPLEMONK.GS_INVENTORY_INNERWEAR union all select replace(\" SKU\",\' \',\'\') SKU ,_airbyte_normalized_at::date DATA_FETCH_DATE ,TRY_TO_DOUBLE(QUANTITY) AVAILABLEINVENTORY ,\'STRINGERS\' GS_CATEGORY ,_airbyte_normalized_at from FUAARK_DB.MAPLEMONK.GS_INVENTORY_STRINGERS union all select replace(SKU,\' \',\'\') SKU ,_airbyte_normalized_at::date DATA_FETCH_DATE ,TRY_TO_DOUBLE(QUANTITY) AVAILABLEINVENTORY ,\'TSHIRT\' GS_CATEGORY ,_airbyte_normalized_at from FUAARK_DB.MAPLEMONK.GS_INVENTORY_TSHIRT union all select replace(SKU,\' \',\'\') SKU ,_airbyte_normalized_at::date DATA_FETCH_DATE ,TRY_TO_DOUBLE(QUANTITY) AVAILABLEINVENTORY ,\'WINTERWEAR\' GS_CATEGORY ,_airbyte_normalized_at from FUAARK_DB.MAPLEMONK.GS_INVENTORY_WINTERWEAR ) ) where rw = 1 ), SKUMASTER as ( select * from (select SKUCODE , NAME , CATEGORY , SUB_CATEGORY , COLOUR , GENDER , row_number() over (partition by SKUCODE order by 1) rw from FUAARK_DB.MAPLEMONK.FINAL_SKU_MASTER ) where rw = 1 ), Inventory_Data as ( select ID.DATA_FETCH_DATE ,ID.SKU ,SKUMASTER.NAME PRODUCTNAME ,NULL AS SIZE ,SKUMASTER.GENDER ,SKUMASTER.SUB_CATEGORY ,ID.GS_CATEGORY ,SKUMASTER.CATEGORY ,SKUMASTER.COLOUR COLOR ,sum(ifnull(AVAILABLEINVENTORY,0)) AVAILABLEINVENTORY from Inventory_Data_Base ID left join SKUMASTER on ID.SKU = SKUMASTER.SKUCODE group by 1,2,3,4,5,6,7,8,9 ), Sales_Data_Daily as ( SELECT order_date, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, GENDER, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM FUAARK_DB.MAPLEMONK.FUAARK_DB_sales_consolidated GROUP BY 1, 2,3,4,5,6 ) , Sales_Data_Max_Weekly as ( WITH WeeklySales AS ( SELECT date_trunc(\'week\', order_date) AS week_Start, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, GENDER, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM FUAARK_DB.MAPLEMONK.FUAARK_DB_sales_consolidated where datediff(day,order_date,current_date()) <=56 GROUP BY 1, 2,3,4,5,6 ) SELECT week_Start, SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, GENDER, QUANTITY, RETURNED_QUANTITY FROM ( SELECT week_Start, SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, GENDER, QUANTITY, RETURNED_QUANTITY, row_number() OVER (PARTITION BY SKU ORDER BY QUANTITY DESC) AS ranking FROM WeeklySales ) ranked_weekly_sales WHERE ranking = 1 ) , Sales_Data_Min_Weekly as ( WITH WeeklySales AS ( SELECT date_trunc(\'week\', order_date) AS week_Start, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, GENDER, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM FUAARK_DB.MAPLEMONK.FUAARK_DB_sales_consolidated where datediff(day,order_date,current_date()) <=56 GROUP BY 1, 2,3,4,5,6 ) SELECT week_Start, SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, GENDER, QUANTITY, RETURNED_QUANTITY FROM ( SELECT week_Start, SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, GENDER, QUANTITY, RETURNED_QUANTITY, row_number() OVER (PARTITION BY SKU ORDER BY QUANTITY) AS ranking FROM WeeklySales ) ranked_weekly_sales WHERE ranking = 1 ) , Sales_56avg_w_Inv as ( select ID.DATA_FETCH_DATE ,ID.SKU SKU ,coalesce(ID.PRODUCTNAME,SD56.PRODUCT_NAME_FINAL) Product_Final_Name ,coalesce(ID.category, SD56.PRODUCT_CATEGORY) CATEGORY ,ID.GS_CATEGORY ,coalesce(ID.SUB_CATEGORY, SD56.PRODUCT_SUB_CATEGORY) SUB_CATEGORY ,coalesce(ID.GENDER, SD56.GENDER) GENDER ,ID.COLOR ,max(ifnull(ID.availableinventory,0)) Available_Inventory ,sum(ifnull(SD56.Quantity,0)) Sold_Quantity_56_Days ,max(ifnull(SWMax.Quantity,0)) Max_Weekly_Sales ,max(ifnull(SWMin.Quantity,0)) Min_Weekly_Sales from Inventory_Data ID left join Sales_Data_Daily SD56 on ID.SKU = SD56.SKU and (datediff(day,try_to_date(SD56.ORDER_DATE),ifnull(ID.DATA_FETCH_DATE,current_date())) BETWEEN 1 AND 56) left join Sales_Data_Max_Weekly SWMax on ID.SKU = SWMax.SKU left join SALES_DATA_MIN_WEEKLY SWMin on ID.SKU = SWMin.SKU group by 1,2,3,4,5,6,7,8 ) , Sales_56_14_Avg_w_Inv as ( select DATA_FETCH_DATE ,SD56.SKU ,SD56.Product_Final_Name ,SD56.Category ,SD56.GS_CATEGORY ,SD56.SUB_CATEGORY ,SD56.GENDER ,SD56.COLOR ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,sum(ifnull(SD14.Quantity,0)) Sold_Quantity_14_Days from Sales_56avg_w_Inv SD56 left join Sales_Data_Daily SD14 on SD56.SKU = SD14.SKU and (datediff(day,try_to_date(SD14.ORDER_DATE),ifnull(SD56.DATA_FETCH_DATE,current_date())) BETWEEN 1 AND 14) group by 1,2,3,4,5,6,7,8,9,10,11,12 ) , Sales_56_14_mtd_Avg_w_Inv as ( select DATA_FETCH_DATE ,SD56.SKU ,SD56.Product_Final_Name ,SD56.Category ,SD56.GS_CATEGORY ,SD56.SUB_CATEGORY ,SD56.GENDER ,SD56.COLOR ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,Sold_Quantity_14_Days ,sum(ifnull(SDmtd.Quantity,0)) sold_quantity_mtd from Sales_56_14_Avg_w_Inv SD56 left join Sales_Data_Daily SDmtd on SD56.SKU = SDmtd.SKU and date_trunc(\'month\',try_to_date(SDmtd.ORDER_DATE)-1) = date_trunc(\'month\',ifnull(SD56.DATA_FETCH_DATE,current_date())) and try_to_date(SDmtd.ORDER_DATE) < ifnull(SD56.DATA_FETCH_DATE,current_date()) group by 1,2,3,4,5,6,7,8,9,10,11,12,13 ) select DATA_FETCH_DATE ,right(DATA_FETCH_DATE::Date,2)-1 mtd_Days ,SD56_14.SKU ,SD56_14.Product_Final_Name ,SD56_14.Category ,SD56_14.GS_CATEGORY ,SD56_14.SUB_CATEGORY ,SD56_14.GENDER ,SD56_14.COLOR ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,Sold_Quantity_14_Days ,sold_quantity_mtd ,sum(ifnull(SD7.Quantity,0)) Sold_Quantity_7_Days from Sales_56_14_mtd_Avg_w_Inv SD56_14 left join Sales_Data_Daily SD7 on SD56_14.SKU = SD7.SKU and (datediff(day,try_to_date(SD7.ORDER_DATE),ifnull(SD56_14.DATA_FETCH_DATE,current_date())) BETWEEN 1 AND 7) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from FUAARK_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        