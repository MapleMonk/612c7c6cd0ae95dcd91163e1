{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.snitch.ORDER_lineitems_fact as select sh_t.market_place, sh_t.marketplace_mapped, sh_t.source, sh_t.order_id, sh_t.order_name, sh_t.SALES_ORDER_ITEM_ID, sh_t.salesorderitemcode, sh_t.order_date, sh_t.order_timestamp, sh_t.order_status, sh_t.sku, sh_t.sku_group, sh_t.PRODUCT_ID, sh_t.currency, sh_t.suborder_QUANTITY, sh_t.DISCOUNT, ifnull(uni_t.tax,0) tax, sh_t.selling_price, sh_t.mrp, sh_t.warehouse, sh_t.source_channel, sh_t.payment_method, sh_t.shipping_quantity, sh_t.discount_code from ( select \'Shopify_India\' as market_place, \'Shopify_India\' as marketplace_mapped, \'SHOPIFY\' as source, ID:: varchar as order_id , name :: varchar as order_name, A.VALUE:id :: varchar AS SALES_ORDER_ITEM_ID, null as salesorderitemcode, CREATED_AT::DATE AS order_date, CREATED_AT::DATETIME AS order_timestamp, CASE WHEN cancelled_at IS NOT NULL THEN \'CANCELLED\' ELSE \'Shopify_Processed\' END AS order_status, A.VALUE:sku::STRING AS SKU, REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, A.VALUE:product_id::STRING AS PRODUCT_ID, currency, A.VALUE:quantity::FLOAT as suborder_QUANTITY, B.VALUE:amount::FLOAT AS DISCOUNT, null as tax, (A.value : price :: float *A.VALUE:quantity::FLOAT )-ifnull((B.value : amount:: float),0) as selling_price, A.value : price :: float as MRP, null as warehouse, \'SHOPIFY\' as source_channel, gateway as payment_method, A.VALUE:quantity::FLOAT as shipping_quantity, replace(D.value:code,\'\"\',\'\') as discount_code FROM Snitch_db.maplemonk.shopifyindia_orders, LATERAL FLATTEN (INPUT => LINE_ITEMS,outer => true) AS A, LATERAL FLATTEN (INPUT => A.VALUE:discount_allocations,outer => true) AS B , lateral flatten (input =>DISCOUNT_CODES,outer => true) as D ) sh_t left join ( select replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(A.Value:code,\'\"\',\'\') as SALES_ORDER_ITEM_ID, A.Value:totalIntegratedGst::float as tax from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A )uni_t on sh_t.order_id = uni_t.order_id and sh_t.SALES_ORDER_ITEM_ID = uni_t.SALES_ORDER_ITEM_ID union all select u_t.*,u_t1.shipping_quantity ,null as discount_code from (select * from (select replace(saleorderdto:channel,\'\"\',\'\') marketplace, case when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%ajio%\' then \'AJIO\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%myntra%\' then \'MYNTRA\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%fynd%\' then \'FYND\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%amazon%\' then \'AMAZON\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%flipkart%\' then \'FLIPKART\' else replace(saleorderdto:channel,\'\"\',\'\') end as marketplace_mapped, replace(saleorderdto:source,\'\"\',\'\') as source, replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(saleorderdto:displayOrderCode,\'\"\',\'\') as order_name, A.Value:id::varchar as SALES_ORDER_ITEM_ID, replace(A.Value:code,\'\"\',\'\') as saleOrderItemCode, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\'))) as order_date, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\')) as order_timestamp, replace(saleorderdto:status,\'\"\',\'\') as ORDER_STATUS, replace(A.Value:itemSku,\'\"\',\'\') as sku, REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, replace(A.Value:channelProductId,\'\"\',\'\') as product_id, replace(saleorderdto:currencyCode,\'\"\',\'\') as currency, A.Value:packetNumber::int as SUBORDER_QUANTITY, A.Value:discount::float as discount, A.Value:totalIntegratedGst::float as tax, A.Value:totalPrice::float as SELLING_PRICE, replace(A.Value:maxRetailPrice,\'\"\',\'\') as MRP, replace(A.Value:facilityName,\'\"\',\'\') as warehouse_name, \'UNICOMMERCE\' as source_channel, null as payment_method from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A) where marketplace != \'SHOPIFY\')u_t left join (select * from (select replace(saleorderdto:code,\'\"\',\'\') ordeR_id, replace(B.value:itemSku, \'\"\',\'\') sku, replace(B.value:quantity, \'\"\',\'\') shipping_quantity, row_number () over (partition by replace(saleorderdto:code,\'\"\',\'\'), replace(B.value:itemSku, \'\"\',\'\') order by 1 ) rw from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A, lateral flatten (input => A.value:items) B) where rw = 1)u_t1 on u_t.order_id = cast(u_t1.order_id as varchar) and u_t.sku = u_t1.sku union all select Branch_Name::varchar as marketplace, Branch_Name::varchar as marketplace_mapped, BRANCH_SHORT_NAME::varchar as source, bill_no::varchar as order_id, new_bill_no::varchar as ordeR_name, replace(A.Value:SO_Item_Order_ID::varchar,\'\"\',\'\') as SALES_ORDER_ITEM_ID, replace(A.Value:SO_Item_Order_ID::varchar,\'\"\',\'\') as saleOrderItemCode, -- bill_date, to_date(bill_date, \'DD/MM/YYYY\') as order_date, null as order_timestamp, case when lower(bill_cancelled) = \'false\' then \'Processed\' else \'Cancelled\' end as ORDER_STATUS, replace(A.Value:AddlItemCode,\'\"\',\'\')::varchar as sku, REVERSE(SUBSTRING(REVERSE(sku::varchar), CHARINDEX(\'-\', REVERSE(sku::varchar)) + 1)) AS sku_group, replace(A.Value:HSN_Code::varchar,\'\"\',\'\') as product_id, \'INR\' as currency, A.Value:Quantity::int as SUBORDER_QUANTITY, (A.Value:CD::float)*-1 as discount, (A.Value:Tax_Amt_1::float) + (A.Value:Tax_Amt_3::float) as tax, A.Value:Net_Amt::varchar as SELLING_PRICE, replace(A.Value:Item_MRP::varchar,\'\"\',\'\') as MRP, replace(A.Value:Godown_Name::varchar,\'\"\',\'\') as warehouse_name, \'LOGICERP\' as source_channel, null as payment_method, A.Value:Quantity::int as shipping_QUANTITY, null as discount_code from snitch_db.maplemonk.LOGICERPConsolidated_GET_SALE_INVOICE, LATERAL FLATTEN (INPUT => LSTITEMS)A; create or replace table snitch_db.snitch.ORDER_lineitems_fact as select l_i.*, p_m.mode_of_payment, p_m.primary_payment_type, Coalesce (p_m.secondary_payment_type,p_m.primary_payment_type) as secondary_payment_type , Coalesce(p_m.tertiary_payment_type,p_m.primary_payment_type) as tertiary_payment_type from snitch_db.snitch.ORDER_LINEITEMS_FACT l_i left join snitch_db.maplemonk.dim_mapping_payment_mode p_m on lower(l_i.PAYMENT_METHOD) = lower(p_m.payments)",
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
                        