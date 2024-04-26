{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_GA_PDP as with abc as ( select order_date, count(distinct REFERENCE_CODE)as total_orders, sum(QUANTITY)as total_quantity, sum(case when lower(status) = \'non cancel\' then QUANTITY end )as non_cancel_quantity from ( select o.order_date, o.REFERENCE_CODE, o.status, o.SKU_CODE, Sum(o.total_quantity) as QUANTITY from ( select date(order_date)as order_date, REFERENCE_CODE, case when lower(FINAL_SHIPPING_STATUS) like \'%cancel%\' then \'cancel\' else \'non cancel\' end as status, SKU_CODE, sum(QUANTITY) as total_quantity from hox_db.maplemonk.HOX_DB_sales_consolidated where lower(MARKETPLACE) like \'%shopify%\' group by 1,2,3,4 )as o where o.SKU_CODE in (\'BLKBE100\', \'BLKBE20\') group by 1,2,3,4 )as o group by 1 order by 1 desc ) select distinct o.*, total_orders, total_quantity, non_cancel_quantity from ( select to_date(date, \'YYYYMMDD\')as event_date, sum(Sessions)as total_sessions, sum(ADDTOCARTS) as ADDTOCARTS, sum(ENGAGEDSESSIONS) as ENGAGEDSESSIONS from hox_db.maplemonk.BLANKO_GA_PDP_DATA where lower(UNIFIEDPAGEPATHSCREEN) like \'%/best-men-perfume/blanko-billionaire-long-lasting-fragrance%\' group by 1 order by 1 desc ) as o left join abc on o.event_date = abc.order_date",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        