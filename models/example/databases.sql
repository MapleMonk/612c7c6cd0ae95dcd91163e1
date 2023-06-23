{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table xyxx_db.maplemonk.shopify_all_payment_details as with shopify_payments as ( select NAME order_name ,case when lower(B.value:receipt) like any (\'%error%\',\'%failed%\') then \'Payment Processing Error\' when lower(B.value:receipt) like \'%refund%\' then \'Refund\' when lower(B.value:receipt) like \'%payment%\' then \'Payment\' else case when lower(replace(B.value:gateway,\'\"\',\'\')) like \'%cash%\' then \'COD\' else \'Others\' end end as Transaction_Type ,replace(B.value:paymentId,\'\"\',\'\') as Payment_ID ,A.CREATEDAT::date Created_date ,A.UPDATEDAT::date Updated_Date ,replace(B.value:formattedGateway,\'\"\',\'\') as Payment_Gateway from xyxx_db.maplemonk.xyxx_shopify_order_payment_details A , lateral flatten (INPUT => transactions) B where lower(Transaction_Type) not in (\'payment processing error\') ) select S.* ,ifnull(RP.Fee,0)/100 Transaction_Fee ,ifnull(RP.Tax,0)/100 Transaction_Tax ,ifnull(RP.AMOUNT,0)/100 Transaction_amount ,RP.Method Payment_Method from shopify_payments S left join (select replace(notes:shopify_order_id,\'\"\',\'\') razor_payment_id, order_id razor_order_id, fee, tax, method, amount, wallet, currency from xyxx_db.maplemonk.xyxx_payments where lower(status) not in (\'failed\') ) RP on S.payment_id = RP.razor_payment_id ; create or replace table xyxx_db.maplemonk.shopify_all_order_level_payment_summary as with payment_summary as ( select NAME order_name, array_agg( Object_construct( \'CREATED_AT\', A.CREATEDAT, \'UPDATED_AT\', A.UPDATEDAT, \'PAYMENT_GATEWAY\', replace(replace(B.value:formattedGateway,\'\"\',\'\'),\'\"\',\'\'), \'PAYMENT_ID\', replace(B.value:paymentId,\'\"\',\'\'), \'TRANSACTION_TYPE\', case when lower(B.value:receipt) like \'%error%\' then \'Payment Processing Error\' when lower(B.value:receipt) like \'%refund%\' then \'Refund\' when lower(B.value:receipt) like \'%payment%\' then \'Payment\' else \'Others\' end ) ) AS PAYMENT_DETAILS from xyxx_db.maplemonk.xyxx_shopify_order_payment_details A , lateral flatten (INPUT => transactions) B group by order_name ), transaction_fee as ( select * from (select *, row_number() over (partition by order_name order by Transaction_Fee) rw from (select order_name ,payment_gateway ,sum(ifnull(Transaction_Fee,0)) Transaction_Fee from xyxx_db.maplemonk.shopify_all_payment_details group by 1,2 ) ) where rw= 1 ) select PS.* ,TF.payment_gateway success_payment_gateway ,TF.Transaction_Fee from payment_summary PS left join transaction_fee TF on PS.order_name = TF.order_name;",
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
                        