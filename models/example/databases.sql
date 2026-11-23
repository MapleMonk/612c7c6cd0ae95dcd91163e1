{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.RAR_Nykaa_SOR_Orders_Fact_Items AS select upper(nykaa_orderno) as order_id, cast(parse_date(\'%d/%m/%y\',order_date) as date) as order_date, coalesce(cast(nsor.MRP as float64),(cast(sk.mrp as float64) * cast(total_qty as int64))) as mrp, cast(orderdetail_total_amount as float64) as total_sales, upper(product_sku) as nykaa_sku, upper(nsor.product_id) as product_id, upper(primary_l2) as nykaa_category, upper(primary_l3) as nykaa_sub_category, upper(orderdetail_sourcewh) as warehouse, cast(total_qty as int64) as order_quantity, cast(RTO_qty as int64) as rto_quantity, cast(Lost_Damaged_Qty as int64) as lost_damaged_quantity, cast(cancelled_qty as int64) as cancelled_quantity, cast(delivered_qty as int64) as delivered_quantity, cast(Open_Qty as int64) as open_quantity, cast(intransit_Qty as int64) as intransit_quantity, cast(dto_Qty as int64) as returned_quantity, upper(sk.product_name) as product_name_final, upper(sk.category) as product_category, cast(sk.cost as float64) as cogs, upper(sk.sku_code) as commonsku, sk.url as image_url, case when cast(RTO_qty as int64) > 0 then \'RTO\' when cast(DTO_qty as int64) > 0 then \'RETURNED\' when cast(lost_damaged_qty as int64) > 0 then \'LOST/DAMAGED\' when cast(cancelled_qty as int64) > 0 then \'CANCELLED\' when cast(delivered_qty as int64) > 0 then \'DELIVERED\' when cast(open_qty as int64) > 0 then \'OPEN\'when cast(intransit_qty as int64) > 0 then \'INTRANSIT\'end as order_status from maplemonk.s3_upload_nykaa_sor_data nsor left join (select *,split(product_id,\'-\')[offset(0)] as nykaa_product_id from maplemonk.RAR_SKU_MASTER where upper(channelname) like \'%NYKAA%\' qualify row_number() over (partition by upper(split(product_id,\'-\')[offset(0)]) order by sku_code) = 1 ) sk on upper(sk.nykaa_product_id) = upper(nsor.product_sku) ;",
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
            