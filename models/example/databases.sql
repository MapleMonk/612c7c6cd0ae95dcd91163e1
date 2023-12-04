{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.snitch.table_for_category_chart as with lineitemwithc_id as (select t1.*,t2.final_size_mapped , t2.product_category from (select t3.*,r_t.returns,r_t.quantity as return_quantity, case when t3.order_status = \'CANCELLED\' then t3.suborder_quantity else 0 end as cancelled_quantity from ( (select cast(t2.customer_id_final as varchar) as customer_id_final ,t2.district,t1.* from (select * from snitch_db.snitch.ORDER_LINEITEMS_FACT)t1 left join (select * from snitch_db.snitch.ORDERS_fact )t2 on t1.order_id = t2.order_id))t3 left join (select order_id,sku,sum(quantity) as quantity , \'True\' as returns from snitch_db.snitch.customer_returns_dim group by order_id,sku) r_t on t3.order_id = r_t.order_id and t3.sku = r_t.sku)t1 left join (select distinct p_t.sku ,m_t.final_size_mapped,p_t.product_category from snitch_db.snitch.product_dim p_t left join snitch_db.maplemonk.dim_mapping_product_dim m_t on p_t.size = m_t.size and p_t.product_category = m_t.product_category)t2 on t1.sku = t2.sku ) select * from lineitemwithc_id",
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
                        