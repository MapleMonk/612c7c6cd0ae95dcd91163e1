{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table BUMMER_DB.MAPLEMONK.BUMMER_DB_inventory_fact_items as with Inventory_Data_Base as ( select * from (select NULL COMPANYID ,NULL COMPANYNAME ,try_to_timestamp(timestamp) CREATIONDATE ,try_to_timestamp(timestamp) LASTUPDATEDATE ,try_to_timestamp(timestamp) DATA_FETCH_DATE ,replace(SKUCODE,\'\\'\',\'\') SKU ,replace(SKUCODE,\'\\'\',\'\') ACCOUNTINGSKU ,replace(SKUCODE,\'\\'\',\'\') PRODUCTID ,replace(SKUCODE,\'\\'\',\'\') COMPANYPRODUCTID ,NULL as MODELNO ,NULL as PRODUCTUNIQUECODE ,UPPER(NAME) PRODUCTNAME ,NULL as DESCRIPTION ,NULL IS_COMBO ,NULL IMAGEURL ,NULL SIZE ,Upper(BRAND) BRAND ,CATEGORYCODE CATEGORY ,NULL COLOR ,NULL WIDTH ,NULL HEIGHT ,NULL LENGTH ,NULL WEIGHT ,NULL MRP ,NULL COST ,FACILITY LOCATION_KEY ,NULL as ACCOUNTINGUNIT ,PUTAWAYPENDING as RESERVEDINVENTORY ,INVENTORY AVAILABLEINVENTORY ,NULL as INVENTORYTHRESHOLD ,NULL as SELLINGPRICETHRESHOLD ,row_number() over (partition by facility,replace(SKU,\'\\'\',\'\'), try_to_date(timestamp)::date order by timestamp ASC) rw from BUMMER_DB.MAPLEMONK.UNICOMMERCE_UNICOMMERCE_BUMMER_GET_INVENTORY_SNAPSHOT where not(lower(facility) like \'bummer_offline\') and not(upper(REPLACE(SKUCODE, \'\\'\', \'\')) like any (\'BUM%\',\'OPM%\',\'MPM%\')) ) where rw = 1 ), Inventory_Data as ( select DATA_FETCH_DATE ,SKU ,PRODUCTID ,PRODUCTNAME ,SIZE ,BRAND ,CATEGORY ,COLOR ,sum(ifnull(AVAILABLEINVENTORY,0)) AVAILABLEINVENTORY from Inventory_Data_Base group by 1,2,3,4,5,6,7,8 ) , Sales_Data_Daily as ( SELECT order_date, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM BUMMER_DB.MAPLEMONK.BUMMER_DB_sales_consolidated GROUP BY 1, 2 ) , Sales_Data_Max_Weekly as ( WITH WeeklySales AS ( SELECT date_trunc(\'week\', order_date) AS week_Start, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM BUMMER_DB.MAPLEMONK.BUMMER_DB_sales_consolidated where datediff(day,order_date,current_date()) <=56 GROUP BY 1, 2 ) SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY FROM ( SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY, row_number() OVER (PARTITION BY SKU ORDER BY QUANTITY DESC) AS ranking FROM WeeklySales ) ranked_weekly_sales WHERE ranking = 1 ) , Sales_Data_Min_Weekly as ( WITH WeeklySales AS ( SELECT date_trunc(\'week\', order_date) AS week_Start, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM BUMMER_DB.MAPLEMONK.BUMMER_DB_sales_consolidated where datediff(day,order_date::date,current_date()) <=56 GROUP BY 1, 2 ) SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY FROM ( SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY, row_number() OVER (PARTITION BY SKU ORDER BY QUANTITY) AS ranking FROM WeeklySales ) ranked_weekly_sales WHERE ranking = 1 ) , Sales_56avg_w_Inv as ( select ID.DATA_FETCH_DATE ,ID.SKU SKU ,ID.productid ,Upper(ID.PRODUCTNAME) Product_Final_Name ,upper(ID.category) Category ,upper(ID.COLOR) COLOR ,max(ifnull(ID.availableinventory,0)) Available_Inventory ,sum(ifnull(SD56.Quantity,0)) Sold_Quantity_56_Days ,max(ifnull(SWMax.Quantity,0)) Max_Weekly_Sales ,max(ifnull(SWMin.Quantity,0)) Min_Weekly_Sales from Inventory_Data ID left join Sales_Data_Daily SD56 on ID.SKU = SD56.SKU and (datediff(day,SD56.ORDER_DATE::date,ifnull(ID.DATA_FETCH_DATE::date,current_date())) BETWEEN 1 AND 56) left join Sales_Data_Max_Weekly SWMax on ID.SKU = SWMax.SKU left join SALES_DATA_MIN_WEEKLY SWMin on ID.SKU = SWMin.SKU group by 1,2,3,4,5,6 ) , Sales_56_14_Avg_w_Inv as ( select DATA_FETCH_DATE ,SD56.SKU ,productid ,Product_Final_Name ,Category ,COLOR ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,sum(ifnull(SD14.Quantity,0)) Sold_Quantity_14_Days from Sales_56avg_w_Inv SD56 left join Sales_Data_Daily SD14 on SD56.SKU = SD14.SKU and (datediff(day,SD14.ORDER_DATE::date,ifnull(SD56.DATA_FETCH_DATE::date,current_date())) BETWEEN 1 AND 14) group by 1,2,3,4,5,6,7,8,9,10 ) , Sales_56_14_mtd_Avg_w_Inv as ( select DATA_FETCH_DATE ,SD56.SKU ,productid ,Product_Final_Name ,Category ,COLOR ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,Sold_Quantity_14_Days ,sum(ifnull(SDmtd.Quantity,0)) sold_quantity_mtd from Sales_56_14_Avg_w_Inv SD56 left join Sales_Data_Daily SDmtd on SD56.SKU = SDmtd.SKU and date_trunc(\'month\',SDmtd.ORDER_DATE::date-1) = date_trunc(\'month\',ifnull(SD56.DATA_FETCH_DATE::date,current_date())) and SDmtd.ORDER_DATE::date < ifnull(SD56.DATA_FETCH_DATE::Date,current_date()) group by 1,2,3,4,5,6,7,8,9,10,11 ) select DATA_FETCH_DATE ,right(DATA_FETCH_DATE::Date,2)-1 mtd_Days ,SD56_14.SKU ,productid ,coalesce(sm.Name,SD56_14.Product_Final_Name) Product_Final_Name ,coalesce(sm.Category,sd56_14.Category) as Category ,sm.gender ,sm.size ,sm.SUB_CATEGORY ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,Sold_Quantity_14_Days ,sold_quantity_mtd ,sum(ifnull(SD7.Quantity,0)) Sold_Quantity_7_Days from Sales_56_14_mtd_Avg_w_Inv SD56_14 left join Sales_Data_Daily SD7 on SD56_14.SKU = SD7.SKU and (datediff(day,SD7.ORDER_DATE::date,ifnull(SD56_14.DATA_FETCH_DATE::date,current_date())) BETWEEN 1 AND 7) left join( select * from (select *,row_number() over(partition by commonsku order by 1)rw from BUMMER_DB.MAPLEMONK.FINAL_SKU_MASTER ) where rw = 1 )sm on lower(sm.commonsku) = lower(SD56_14.sku) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from BUMMER_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            