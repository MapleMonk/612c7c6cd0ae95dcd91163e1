{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items as select CHANNELTYPEDESC marketplace ,replace(A.value:brand,\'\"\',\'\') Brand ,orderid order_id ,orderno reference_code ,case when orderdate = \'\' then null else to_timestamp(orderdate,\'dd/mm/yyyy HH24:MI:SS\') end ordeR_date ,replace(A.value:status,\'\"\',\'\') order_status ,upper(billcity) city ,upper(billstate) state ,upper(billcountry) country ,billpincode pincode ,billname name ,billemail1 email ,billphone1 phone ,case when shipbydate = \'\' then null else to_timestamp(shipByDate,\'dd/mm/yyyy HH24:MI:SS\') end ship_by_date ,case when updatedDate = \'\' then null else to_timestamp(updatedDate,\'dd/mm/yyyy HH24:MI:SS\') end updated_date ,paymentmethod ,replace(A.value:lineno,\'\"\',\'\') saleOrderItemCode ,replace(A.value:orderQty,\'\"\',\'\') SUBORDER_QUANTITY ,replace(A.value:returnQty,\'\"\',\'\') return_quantity ,replace(A.value:shippedQty,\'\"\',\'\') shipped_quantity ,replace(A.value:sku,\'\"\',\'\') sku ,replace(A.value:skuName,\'\"\',\'\') product_name ,replace(A.value:taxAmount,\'\"\',\'\') Tax ,replace(A.value:unitPrice,\'\"\',\'\') selling_price ,replace(A.value:discountAmt,\'\"\',\'\') Discount ,replace(A.value:cancelledQty,\'\"\',\'\') cancelled_quantity ,datediff(hour,ordeR_date,ship_by_date) estimated_tat ,datediff(hour,order_date,updated_date) actual_tat from (select * from (select replace(_airbyte_data:channelTypeDesc,\'\"\',\'\') channelTypeDesc, _airbyte_data:items items, replace(_airbyte_data:orderNo,\'\"\',\'\') orderNo, replace(_airbyte_data:orderId,\'\"\',\'\') orderId, replace(_airbyte_data:orderDate,\'\"\',\'\') orderDate, replace(_airbyte_data:billCity,\'\"\',\'\') billCity, replace(_airbyte_data:billState,\'\"\',\'\') billState, replace(_airbyte_data:billCountry,\'\"\',\'\') billCountry, replace(_airbyte_data:billPincode,\'\"\',\'\') billPincode, replace(_airbyte_data:billName,\'\"\',\'\') billName, replace(_airbyte_data:billEmail1,\'\"\',\'\') billEmail1, replace(_airbyte_data:billPhone1,\'\"\',\'\') billPhone1, replace(_airbyte_data:shipByDate,\'\"\',\'\') shipByDate, replace(_airbyte_data:updatedDate,\'\"\',\'\') updatedDate, replace(_airbyte_data:paymentMethod,\'\"\',\'\') paymentMethod, row_number() over (partition by _airbyte_data:id order by TO_TIMESTAMP(replace(_airbyte_data:updatedDate,\'\"\',\'\'), \'DD/MM/YYYY HH24:MI:SS\') desc) rw from bsc_db.maplemonk._airbyte_raw_bsc_vinculum_orders ) where rw = 1), LATERAL FLATTEN (INPUT => items)A ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        