{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table italiancolony_db.maplemonk.returns_reconciliation_italiancolony as select a.*, b.current_Status clickpost_status from ( select replace(saleorderdto:displayOrderCode,\'\"\',\'\') ordeR_name, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\')) order_Date, A.value:trackingNumber::string awb, A.value:trackingStatus::string UC_status, A.value:returnCompletedDate return_completed_date, B.value:itemSku::string sku, B.value:inventoryType::string inventory_type from italiancolony_db.MAPLEMONK.Unicommerce_Italian_Colony_unicommerce_GET_ORDERS_BY_IDS_TEST C , LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ) a left join ( select awb_number ,current_status from italiancolony_db.MAPLEMONK.italiancolony_DB_CLICKPOST_FACT_ITEMS )b on a.awb = b.awb_number where lower(current_status) like \'%delivered%\' and return_completed_date is null ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from italiancolony_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        