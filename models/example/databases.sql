{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.MAPLEMONK.SHOPIFY_CUSTOMER_PRODUCT_MIGRATION_XYXX as with Order_w_Purchase_Number as ( select customer_id_final ,order_date order_Date ,product_category Product_Category ,name customer_name ,email ,phone ,order_name ,row_number() over (partition by customer_id_final order by order_Date asc, selling_price desc) rw ,dense_rank() over (partition by customer_id_final order by order_Date) purchase_number from (select customer_id_final ,email ,phone ,name ,order_date order_date ,order_name ,product_category ,sum(selling_price) selling_price from xyxx_db.maplemonk.sales_consolidated_xyxx where lower(shop_name) like \'%shopify%\' and not (lower(order_status) like \'%cancel%\' or lower(final_status) like \'%cancel%\') group by 1,2,3,4,5,6,7 ) ) select ao.* ,fo.order_Date First_order_Date ,fo.product_category First_Product_Category ,fo.order_name First_Purchase_Order_Name from (select * from Order_w_Purchase_Number ) ao left join (select * from Order_w_Purchase_Number where rw = 1) fo on ao.customer_id_final = fo.customer_id_final ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        