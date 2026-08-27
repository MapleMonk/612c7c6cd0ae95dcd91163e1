{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.prolicious_blinkit_Fact_Items AS SELECT CAST(S_No_ AS INT64) AS S_No, CAST(Item_Id AS STRING) AS Item_ID, CAST(HSN_Code AS STRING) AS HSN_Code, CAST(MRP__Rs_ AS FLOAT64) AS MRP, CAST(Order_Id AS STRING) AS Order_ID, CAST(Quantity AS INT64) AS Quantity, CAST(REPLACE(Total_Tax, \'-\', \'0\') AS FLOAT64) AS Total_Tax, cast(left(Order_Date,10) as date) AS Order_Date, CAST(Order_Status AS STRING) AS Order_Status, CAST(s.Product_Name AS STRING) AS Product_Name, CAST(Supply_City AS STRING) AS Supply_City, coalesce(qc.city_name,CAST(Customer_City AS STRING)) AS city, qc.state_name as state, CAST(Total_Gross_Bill_Amount AS FLOAT64) AS Selling_Price, CAST(Total_Gross_Bill_Amount AS FLOAT64) AS Selling_Price_Rs, CAST(Variant_Description AS STRING) AS Variant_Description, CAST(Total_Gross_Bill_Amount AS FLOAT64) AS Total_Gross_Bill_Amount, item_id as product_id, p.master_sku as commonsku, coalesce(p.category) as product_category, upper(trim(p.product)) as product, upper(p.product_name) as product_name_final, cast(null as string) as nature, p.pack_variant, flavour,subcat_1, subcat_2, sku_type, Cat from maplemonk.Prolicious_Blinkit_sales s LEFT JOIN (SELECT blinkit_sku, master_sku, product_name, category, brand, product, pack_variant,flavour,subcat_1, subcat_2, sku_type, Cat from maplemonk.final_latest_SKU_MASTER where blinkit_sku is not null qualify row_number() over (partition by blinkit_sku order by master_sku) = 1 ) p on upper(p.blinkit_sku) = upper(REPLACE(s.Item_Id, \'\"\', \'\')) LEFT JOIN (select upper(trim(city)) city_name, upper(trim(Quick_Commerce_City)) as Quick_Commerce_City, upper(trim(state)) state_name, from maplemonk.gs_qc_location_mapping qualify row_number() over (partition by upper(trim(Quick_Commerce_City)) order by 1 desc)=1 ) qc on qc.city_name = upper(s.Customer_City) qualify row_number() over (partition by order_id,item_id order by order_date desc) = 1 ;",
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
            