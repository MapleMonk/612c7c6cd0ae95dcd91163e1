{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.flash_collection_1 as ( with bg_inv as ( select sku_group,inventory as bg_inventory, from snitch_db.maplemonk.flash_sale_1_flash_sale1 ), current_inv as ( select sku_group,available_units as current_inv from snitch_db.maplemonk.AVAILABILITY_MASTER_V2 ), sales_data as ( select sku_group,product_name,sum(quantity) as gross_sales,sum(quantity-rto_quant-dto_quant-cancel_quant) as net_sales, div0(sum(quantity-rto_quant-dto_quant-cancel_quant),DATEDIFF(day, \'2024-03-20\', CURRENT_DATE)) as ros from snitch_db.maplemonk.fact_items_snitch where order_timestamp::date >= \'2024-03-20\' group by 1,2 ), main_data as ( select a.*,b.product_name,b.gross_sales,b.net_sales,b.ros from bg_inv a left join sales_data b on a.sku_group = b.sku_group where b.gross_sales is not null ) select a.*,b.current_inv,round(div0(b.current_inv,a.ros),0) as days_to_sold_out from main_data a left join current_inv b on a.sku_group = b.sku_group ); create or replace table snitch_db.maplemonk.flash_collection_2 as ( with bg_inv as ( select \"Tail but not in collection\" as sku_group,name as product_name,category,inv as bg_inv from snitch_db.maplemonk.flash_sale_1_flash_sale2 ), current_inv as ( select sku_group,available_units as current_inv from snitch_db.maplemonk.AVAILABILITY_MASTER_V2 ), sales_data as ( select sku_group,sum(quantity) as gross_sales,sum(quantity-rto_quant-dto_quant-cancel_quant) as net_sales, div0(sum(quantity-rto_quant-dto_quant-cancel_quant),DATEDIFF(day, \'2024-06-28\', CURRENT_DATE)) as ros from snitch_db.maplemonk.fact_items_snitch where order_timestamp::date >= \'2024-06-28\' group by 1 ), main_data as ( select a.*,b.gross_sales,b.net_sales,b.ros from bg_inv a left join sales_data b on a.sku_group = b.sku_group where b.gross_sales is not null ) select a.*,b.current_inv,round(div0(b.current_inv,a.ros),0) as days_to_sold_out from main_data a left join current_inv b on a.sku_group = b.sku_group );",
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
                        