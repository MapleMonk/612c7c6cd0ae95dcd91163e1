{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table Maplemonk.Atovio_Distributors_Order_FactItems as SELECT CAST(qty AS INT64) AS Quantity, CAST(sku AS STRING) AS sku, CAST(year AS INT64) AS year, CAST(month AS STRING) AS month, CAST(notes AS STRING) AS notes, CAST(owner AS STRING) AS owner, CAST(stage AS STRING) AS stage, CAST(channel AS STRING) AS channel, CAST(net_qty AS INT64) AS net_qty, CAST(REPLACE(gst_rate, \'%\', \'\') AS FLOAT64) AS gst_rate, CAST(location AS STRING) AS location, CAST(order_id AS STRING) AS order_id, CAST(FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%d-%m-%Y\', order_date)) AS DATE) AS order_date, CAST(client_name AS STRING) AS client_name, CAST(partner_type AS STRING) AS partner_type, COALESCE(CAST(p.product_name AS STRING),CAST(fi.product_name AS STRING)) AS product_name, CAST(returned_qty AS INT64) AS returned_qty, CAST(order_line_id AS STRING) AS order_line_id, CAST(payment_status AS STRING) AS payment_status, CAST(REPLACE(REPLACE(per_unit_ex_gst, \'₹\', \'\'), \',\', \'\') AS FLOAT64) AS per_unit_ex_gst, COALESCE(p.category,CAST(fi.product_category AS STRING)) AS product_category, CAST(REPLACE(REPLACE(line_value_ex_gst, \'₹\', \'\'), \',\', \'\') AS FLOAT64) AS Selling_price, CAST(REPLACE(REPLACE(per_unit_incl_gst, \'₹\', \'\'), \',\', \'\') AS FLOAT64) AS per_unit_incl_gst, CAST(p.sub_category AS STRING) AS product_sub_category ,p.commonsku ,p.style ,cast(mrp.mrp as float64)*CAST(qty AS INT64) mrp_sales ,cast(mrp.msp as float64)*CAST(qty AS INT64) msp_sales ,cast(mrp.cogs as float64)*CAST(qty AS INT64) total_cogs ,cast(mrp.mrp as float64)*CAST(qty AS INT64) - ifnull(CAST(REPLACE(REPLACE(line_value_ex_gst, \'₹\', \'\'), \',\', \'\') AS FLOAT64),0) discount_mrp FROM `Maplemonk.Atovio_GS_DB_Distributors_Order` fi left join (select * from (select upper(trim(product_title)) as Product_name, upper(trim(category)) category, upper(trim(sub_category)) sub_category, upper(trim(style)) style , upper(trim(primarykey)) as commonsku, row_number()over (partition by upper(trim(primarykey)) order by length(ifnull(upper(trim(primarykey)),\'\')) desc) rw from neon-poetry-482906-j7.Maplemonk.atovio_sku_master) where rw = 1 ) p on lower(replace(fi.sku,\' \',\'\')) = lower(replace(p.commonsku,\' \',\'\')) left join (select * from maplemonk.mapping_sku_mrp_cogs qualify row_number() over (partition by sku_code order by 1) = 1) mrp on p.commonsku = mrp.sku_code ;",
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
            