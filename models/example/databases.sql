{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace table maplemonk.other_marketplace_Sales_Fact_items as select COALESCE( SAFE.PARSE_DATE(\'%d/%m/%y\', Invoice_Date), SAFE.PARSE_DATE(\'%d/%m/%Y\', Invoice_Date), SAFE.PARSE_DATE(\'%Y-%m-%d\', Invoice_Date), SAFE.PARSE_DATE(\'%m/%d/%Y\', Invoice_Date), SAFE.PARSE_DATE(\'%d-%b-%y\', Invoice_Date) ) AS Order_date ,upper(trim(sku)) as SKU ,cast(Quantity as int64) as Quantity ,cast(Item_Total as float64) as Selling_price ,upper(trim(Customer_Name)) Customer_Name ,Invoice_Number as order_id ,cast(Item_Tax_Amount as float64) Tax ,\'Retail\' as Marketplace ,p.name AS product_name_final ,(UPPER(cast(p.CATEGORY as string))) AS product_category ,UPPER(cast(p.sub_category as string)) AS product_sub_category ,commonsku from `MAPLEMONK.mamanourish_db_Other_Marketplace_Sales` fi left join (select * from (select marketplace_sku skucode ,product_name name, category,sub_category ,sku_code commonsku , row_number()over (partition by replace(marketplace_sku,\' \',\'\') order by length(ifnull(replace(marketplace_sku,\' \',\'\'),\'\')) desc) rw from maplemonk.final_sku_master ) where rw = 1 ) p on lower(replace(fi.sku,\' \',\'\')) = lower(replace(p.skucode,\' \',\'\'))",
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
            