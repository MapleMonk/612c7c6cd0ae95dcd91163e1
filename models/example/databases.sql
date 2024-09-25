{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table andamen_db.MAPLEMONK.andamen_db_inventory_fact_items as with Inventory_Data_Base as ( select * from (select COMPANYID ,COMPANYNAME ,try_to_timestamp(CREATIONDATE) CREATIONDATE ,try_to_timestamp(LASTUPDATEDATE) LASTUPDATEDATE ,try_to_timestamp(DATA_FETCH_DATE) DATA_FETCH_DATE ,replace(SKU,\'\\'\',\'\') SKU ,ACCOUNTINGSKU ,PRODUCTID ,COMPANYPRODUCTID ,MODELNO ,PRODUCTUNIQUECODE ,PRODUCTNAME ,DESCRIPTION ,IS_COMBO ,IMAGEURL ,upper(case when SIZE in (\'30\',\'31\') then \'S\' when SIZE in (\'32\',\'33\') then \'M\' when SIZE in (\'34\') then \'L\' when SIZE in (\'36\') then \'XL\' when SIZE in (\'38\') then \'XXL\' else SIZE end) SIZE ,BRAND ,Upper(CATEGORY) CATEGORY ,Upper(COLOR) COLOR ,WIDTH ,HEIGHT ,LENGTH ,WEIGHT ,MRP ,COST ,LOCATION_KEY ,ACCOUNTINGUNIT ,RESERVEDINVENTORY ,AVAILABLEINVENTORY ,INVENTORYTHRESHOLD ,SELLINGPRICETHRESHOLD ,row_number() over (partition by companyid,location_key,replace(SKU,\'\\'\',\'\'), productid, try_to_date(data_fetch_date) order by DATA_FETCH_DATE desc) rw from andamen_db.MAPLEMONK.easyecom_easyecom_andamen_inventory_details ) where rw = 1 ), Inventory_Data as ( select DATA_FETCH_DATE ,SKU ,ACCOUNTINGSKU ,PRODUCTID ,PRODUCTNAME ,SIZE ,BRAND ,CATEGORY ,COLOR ,WIDTH ,sum(ifnull(AVAILABLEINVENTORY,0)) AVAILABLEINVENTORY from Inventory_Data_Base group by 1,2,3,4,5,6,7,8,9,10 ) , Sales_Data_Daily as ( SELECT order_date, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM andamen_db.MAPLEMONK.andamen_db_sales_consolidated GROUP BY 1, 2 ) , Sales_Data_Max_Weekly as ( WITH WeeklySales AS ( SELECT date_trunc(\'week\', order_date) AS week_Start, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM andamen_db.MAPLEMONK.andamen_db_sales_consolidated where datediff(day,order_date,current_date()) <=56 GROUP BY 1, 2 ) SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY FROM ( SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY, row_number() OVER (PARTITION BY SKU ORDER BY QUANTITY DESC) AS ranking FROM WeeklySales ) ranked_weekly_sales WHERE ranking = 1 ) , Sales_Data_Min_Weekly as ( WITH WeeklySales AS ( SELECT date_trunc(\'week\', order_date) AS week_Start, REPLACE(SKU_CODE, \'\\'\', \'\') AS SKU, SUM(IFNULL(quantity, 0)) AS QUANTITY, SUM(IFNULL(returned_quantity, 0)) AS RETURNED_QUANTITY FROM andamen_db.MAPLEMONK.andamen_db_sales_consolidated where datediff(day,order_date,current_date()) <=56 GROUP BY 1, 2 ) SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY FROM ( SELECT week_Start, SKU, QUANTITY, RETURNED_QUANTITY, row_number() OVER (PARTITION BY SKU ORDER BY QUANTITY) AS ranking FROM WeeklySales ) ranked_weekly_sales WHERE ranking = 1 ) , Sales_56avg_w_Inv as ( select ID.DATA_FETCH_DATE ,ID.SKU SKU ,ID.ACCOUNTINGSKU ,ID.productid ,Upper(ID.PRODUCTNAME) Product_Final_Name ,upper(ID.category) Category ,upper(ID.COLOR) COLOR ,SIZE ,WIDTH ,max(ifnull(ID.availableinventory,0)) Available_Inventory ,sum(ifnull(SD56.Quantity,0)) Sold_Quantity_56_Days ,max(ifnull(SWMax.Quantity,0)) Max_Weekly_Sales ,max(ifnull(SWMin.Quantity,0)) Min_Weekly_Sales from Inventory_Data ID left join Sales_Data_Daily SD56 on ID.SKU = SD56.SKU and (datediff(day,try_to_date(SD56.ORDER_DATE),ifnull(ID.DATA_FETCH_DATE,current_date())) BETWEEN 1 AND 56) left join Sales_Data_Max_Weekly SWMax on ID.SKU = SWMax.SKU left join SALES_DATA_MIN_WEEKLY SWMin on ID.SKU = SWMin.SKU group by 1,2,3,4,5,6,7,8,9 ) , Sales_56_14_Avg_w_Inv as ( select DATA_FETCH_DATE ,SD56.SKU ,SD56.ACCOUNTINGSKU ,productid ,Product_Final_Name ,Category ,COLOR ,SIZE ,WIDTH ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,sum(ifnull(SD14.Quantity,0)) Sold_Quantity_14_Days from Sales_56avg_w_Inv SD56 left join Sales_Data_Daily SD14 on SD56.SKU = SD14.SKU and (datediff(day,try_to_date(SD14.ORDER_DATE),ifnull(SD56.DATA_FETCH_DATE,current_date())) BETWEEN 1 AND 14) group by 1,2,3,4,5,6,7,8,9,10,11,12,13 ) , Sales_56_14_mtd_Avg_w_Inv as ( select DATA_FETCH_DATE ,SD56.SKU ,SD56.ACCOUNTINGSKU ,productid ,Product_Final_Name ,Category ,COLOR ,SIZE ,WIDTH ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,Sold_Quantity_14_Days ,sum(ifnull(SDmtd.Quantity,0)) sold_quantity_mtd from Sales_56_14_Avg_w_Inv SD56 left join Sales_Data_Daily SDmtd on SD56.SKU = SDmtd.SKU and date_trunc(\'month\',try_to_date(SDmtd.ORDER_DATE)-1) = date_trunc(\'month\',ifnull(SD56.DATA_FETCH_DATE,current_date())) and try_to_date(SDmtd.ORDER_DATE) < ifnull(SD56.DATA_FETCH_DATE,current_date()) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14 ) select DATA_FETCH_DATE ,right(DATA_FETCH_DATE::Date,2)-1 mtd_Days ,SD56_14.SKU ,SD56_14.ACCOUNTINGSKU ,productid ,skumaster.skucode SKU_CODE ,SD56_14.Product_Final_Name ,skumaster.name PRODUCT_MAPPED_NAME ,coalesce(skumaster.category, SD56_14.Category) category ,COLOR ,SIZE ,WIDTH ,skumaster.SUB_CATEGORY ,skumaster.COLLECTION ,skumaster.SEASON ,skumaster.LAUNCH_DATE ,Available_Inventory ,Max_Weekly_Sales ,Min_Weekly_Sales ,Sold_Quantity_56_Days ,Sold_Quantity_14_Days ,sold_quantity_mtd ,sum(ifnull(SD7.Quantity,0)) Sold_Quantity_7_Days from Sales_56_14_mtd_Avg_w_Inv SD56_14 left join Sales_Data_Daily SD7 on SD56_14.SKU = SD7.SKU and (datediff(day,try_to_date(SD7.ORDER_DATE),ifnull(SD56_14.DATA_FETCH_DATE,current_date())) BETWEEN 1 AND 7) left join (select * from (select upper(commonsku) skucode , upper(product_name) name , upper(category) category , upper(sub_category) sub_category , upper(collection) Collection , upper(season) season , try_to_date(launch_date, \'DD Mon YYYY\') LAUNCH_DATE , row_number() over (partition by upper(commonsku) order by 1) rw from andamen_db.MAPLEMONK.sku_master) where rw = 1) skumaster on lower(coalesce(SD56_14.SKU, trim(SPLIT_PART(SPLIT_PART(SPLIT_PART(SD56_14.SKU, \'-\', 1),\' -\',1),\'-\',1)))) = lower(skumaster.skucode) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from andamen_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            