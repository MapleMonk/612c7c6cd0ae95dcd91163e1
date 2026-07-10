{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_EBO_Intransit_Inventory_Report AS With SaleOrder AS ( SELECT Item_SKU_Code AS SKU, Channel_Product_Id AS Product_Id, Order_Date_as_dd_mm_yyyy_hh_MM_ss AS Initial_Order_Date, cast(nullif(Order_Date_as_dd_mm_yyyy_hh_MM_ss,\"\") as datetime) AS Order_Date, replace(Sale_Order_Code,\'`\',\'\') as Order_Id, UPPER(REPLACE(Channel_Name,\" \",\"_\")) AS Marketplace, COUNT(Sale_Order_Item_Code) AS Quantity FROM `MapleMonk.zouk_db_updated_get_sale_orders` b LEFT JOIN (select * from (Select * , row_number() over (partition by lower(marketplace) order by 1) rw from `MapleMonk.Zouk_db_New_Marketplace_Mapping` ) where rw =1 ) mm ON trim(UPPER(REPLACE(b.Channel_Name,\" \",\"_\"))) = trim(Upper(mm.marketplace)) WHERE UPPER(REPLACE(Channel_Name,\" \",\"_\")) like \'%EBO%\' and not(lower(Sale_Order_Item_Status) like any (\'%unfulfil%\',\'%cancel%\')) GROUP BY 1,2,3,4,5,6 ) , Purchase_Orders AS ( SELECT * FROM ( SELECT PO_Code, UPPER(SPLIT(PO_Code, \'_ZOUK\')[OFFSET(0)]) as Clean_PO_Code, Item_SkuCode, Facility, SAFE_CAST(Recieved_Quantity AS FLOAT64) AS Recieved_Quantity, SAFE_CAST(Rejected_Quantity AS FLOAT64) AS Rejected_Quantity, Purchase_Order_Status, row_number()over (partition by lower(PO_Code),lower(Item_SkuCode) order by DATETIME(TIMESTAMP(Updated), \"Asia/Kolkata\") DESC) rw FROM `MapleMonk.zouk_db_get_purchase_orders` WHERE lower(Vendor_Name) like \'%ebo%\' ) WHERE rw = 1 ) SELECT s.*, Facility, Recieved_Quantity, Rejected_Quantity, Purchase_Order_Status, Upper(SM.Collection) as Collection, Upper(SM.Print) as Print, Upper(SM.Category) as Product_Category, Upper(SM.Name) AS Product_Name_Final, Upper(SM.Commonsku) AS Commonsku FROM SaleOrder s LEFT JOIN Purchase_Orders p ON upper(s.Order_Id) = upper(p.clean_PO_code) AND upper(s.SKU) = upper(p.Item_SkuCode) LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY commonsku ORDER BY 1) AS rw FROM MAPLEMONK.FINAL_SKU_MASTER ) WHERE rw = 1 ) SM ON LOWER(s.SKU) = LOWER(SM.commonsku) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            