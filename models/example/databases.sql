{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE EGGOZDB.MAPLEMONK.RETAILER_GROWTH_TABLE as SELECT R.CODE, COUNT(DISTINCT O.ID), SUM(O.ORDER_PRICE_AMOUNT), SUM(O.ORDER_PRICE_AMOUNT)/COUNT(DISTINCT O.ID), sum(pay_amount) Collections, SUM(O.ORDER_PRICE_AMOUNT)-sum(pay_amount), date(timestampadd(minute,330,O.date)) Collection_Date, area_classification FROM EGGOZDB.MAPLEMONK.MY_SQL_ORDER_ORDER O LEFT join EGGOZDB.MAPLEMONK.MY_SQL_RETAILER_RETAILER R ON O.RETAILER_ID=R.ID LEFT JOIN EGGOZDB.MAPLEMONK.MY_SQL_PAYMENT_INVOICE P ON P.ORDER_ID=O.ID LEFT JOIN EGGOZDB.MAPLEMONK.MY_SQL_PAYMENT_PAYMENT PP ON PP.INVOICE_ID=P.ID GROUP by R.CODE, area_classification, date(timestampadd(minute,330,O.date))",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        