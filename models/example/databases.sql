{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table SNITCH_DB.maplemonk.Weight_reconciliation as With ClickPost_data as ( select AWB_NUMBER, PICKUP_DATE::date as pickup_date, CURRENT_STATUS, COURIER_PARTNER, PAYMENT_METHOD from SNITCH_DB.SNITCH.SLEEPYCAT_DB_CLICKPOST_FACT_ITEMS ), Unicommerce_data as ( select awb, order_id, pincode, shipping_courier, zone, sum(SUBORDER_QUANTITY) as quantity, sum(selling_price1) as selling_price, sum(weight1) as weight, div0(weight+99,500) as slab, ceil(SLAB * 2) / 2 AS RoundedSlab from ( select awb, order_id, pincode, uf.sku, SUBORDER_QUANTITY, selling_price as selling_price1, SHIPPING_COURIER , pd.\"WEIGHT(KG)\"*1000*SUBORDER_QUANTITY as weight1, p.zone from (select * from snitch_db.maplemonk.unicommerce_fact_items_snitch where MARKETPLACE_MAPPED = \'SHOPIFY\' )uf left join snitch_db.snitch.product_dim pd on pd.sku = uf.sku left join snitch_db.maplemonk.pincodemappingzoneupdated p on uf.pincode = p.Delivery_Postcode ) group by 1,2,3,4,5 ) select cd.*,ud.*,case when payment_method = \'COD\' and selling_price*0.025>20 then selling_price*0.025 when payment_method = \'COD\' and selling_price*0.025<=20 then 20 else 0 end as Collection_fee from ClickPost_data cd left join unicommerce_data ud on cd.awb_number = ud.awb",
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
                        