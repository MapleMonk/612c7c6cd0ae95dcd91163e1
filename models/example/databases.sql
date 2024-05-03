{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.Store_returns_fact_items as WITH Returns_consolidated as ( select Branch_Name, BRANCH_SHORT_NAME, bill_no, new_bill_no, retail_cust_mobile_no, retail_cust_name, retail_cust_email_id, LSTITEMS, bill_cancelled, bill_date, bill_time from SNITCH_DB.MAPLEMONK.LOGICERP_FY2324_GET_RETURNS where bill_date < \'2024-04-01\' and new_bill_no not in (select distinct new_bill_no FROM snitch_db.maplemonk.logicerpnew_get_sale_return_invoice where bill_date >= \'2024-04-01\') union all select Branch_Name, BRANCH_SHORT_NAME, bill_no, new_bill_no, retail_cust_mobile_no, retail_cust_name, retail_cust_email_id, LSTITEMS, bill_cancelled, bill_date, bill_time FROM snitch_db.maplemonk.logicerpnew_get_sale_return_invoice where bill_date >= \'2024-04-01\' ) select o.*, coalesce(e.product_name, o.product_name) producT_name_shopify, null pla_spends, null banner_spends, null sales_target, null spends_target, uam.CATEGORY as Category_merge, uam.sleeve_type, uam.collar_type, uam.FABRIC, uam.hem, uam.design, uam.closure, uam.fit, uam.occassion, uam.color, am.sku_class, am.category_class from ( select Branch_Name::varchar as marketplace, Branch_Name::varchar as marketplace_mapped, BRANCH_SHORT_NAME::varchar as source, bill_no::varchar as order_id, new_bill_no::varchar as ordeR_name, retail_cust_mobile_no::varchar as phone, retail_cust_name::varchar as name, retail_cust_email_id::varchar as email, null as SHIPPING_LAST_UPDATE_DATE, null as SHIPPING_LAST_UPDATE_timestamp, replace(A.Value:AddlItemCode,\'\"\',\'\')::varchar as sku, SPLIT_PART(sku, \'-\', -1) AS Size, REVERSE(SUBSTRING(REVERSE(sku::varchar), CHARINDEX(\'-\', REVERSE(sku::varchar)) + 1)) AS sku_group, replace(A.Value:HSN_Code::varchar,\'\"\',\'\') as product_id, null as product_name, \'INR\' as currency, null as address_line1, null as address_line2, null as city, null as state, null as country, null as pincode, case when lower(bill_cancelled) = \'false\' then \'Processed\' else \'Cancelled\' end as ORDER_STATUS, to_date(bill_date, \'DD/MM/YYYY\') as order_date, TRY_TO_TIMESTAMP(bill_time, \'DD/MM/YYYY HH12:MI AM\') order_timestamp, null as shipping_price, A.Value:Quantity::int as SUBORDER_QUANTITY, A.Value:Lot_Basic_Rate ::int as COGS_PRICE, A.Value:Quantity::int as shipping_quantity, (A.Value:CD::float)*-1 as discount, (A.Value:Tax_Amt_1::float) + (A.Value:Tax_Amt_3::float) as tax, A.Value:Net_Amt::varchar as SELLING_PRICE, null as shippingPackageCode, null as shippingPackageStatus, replace(A.Value:Godown_Name::varchar,\'\"\',\'\') as warehouse_name, replace(A.Value:SO_Item_Order_ID::varchar,\'\"\',\'\') as saleOrderItemCode, replace(A.Value:Item_MRP::varchar,\'\"\',\'\') as MRP, replace(A.Value:SO_Item_Order_ID::varchar,\'\"\',\'\') as SALES_ORDER_ITEM_ID, null as courier, null as shipping_courier, null as shipping_status, null as created_timestamp, null as picking_timestamp, null as picked_timestamp, null as packed_timestamp, null as manifested_timestamp, null as dispatched_timestamp, to_date(bill_date, \'DD/MM/YYYY\') Dispatch_date, to_date(bill_date, \'DD/MM/YYYY\') Delivered_Date, null as delivered_timestamp, null Return_Date, 0 as days_to_dispatch, null as awb, null as return_flag, null as return_quantity, case when order_status = \'CANCELLED\' then suborder_quantity else 0 end::int as cancelled_quantity, case when dense_rank()over(partition by phone,order_date order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE(product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, 0 as days_in_shipment, 0 as dispatch_to_delivery_days, null as invoice_date, null as cost, CASE WHEN replace(A.Value:Item_Group_Name_1::varchar,\'\"\',\'\') = \'\' THEN \'NA\' WHEN lower(replace(A.Value:Item_Group_Name_1::varchar,\'\"\',\'\')) = \'jeans\' THEN \'Denim\' WHEN lower(replace(A.Value:Item_Group_Name_1::varchar,\'\"\',\'\')) = \'pant\' then \'Pants\' ELSE replace(A.Value:Item_Group_Name_1::varchar,\'\"\',\'\') END AS category from Returns_consolidated, LATERAL FLATTEN (INPUT => LSTITEMS)A) o left join ( select * from ( select replace(b.value:\"sku\",\'\"\',\'\') as SKU, title product_name, product_type, row_number() over (partition by sku order by product_name) rw from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS, lateral flatten (INPUT => variants)b ) where rw = 1 ) e on lower(o.sku) = lower(e.sku) left join ( SELECT * FROM ( select distinct sku_group, CATEGORY, sleeve_type, collar_type, FABRIC, hem, design, closure, fit, occassion, color, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY 1) RN from snitch_db.maplemonk.unicommerce_availability_merge ) WHERE RN=1 ) uam on o.sku_group = uam.sku_group left join ( SELECT * FROM ( select distinct sku_group, sku_class, category as category_class, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY 1) RN from snitch_db.maplemonk.availability_master ) WHERE RN=1 ) am on o.sku_group = am.sku_group",
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
                        