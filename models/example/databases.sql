{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from sleepycat_db.maplemonk.sleepycat_db_sales_consolidated; Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from sleepycat_db.maplemonk.sleepycat_db_sales_cost_source; Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from sleepycat_db.maplemonk.sleepycat_db_order_fulfillment_report; Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from sleepycat_db.maplemonk.sleepycat_db_summary_sku_sales; Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from sleepycat_db.maplemonk.sleepycat_db_marketing_consolidated; Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CUSTOMER_RETENTION; Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_monthly_retention; Create Or Replace Table sleepycat_db.sleepycat.sleepycat_db_sales_consolidated as select * from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CUSTOMER_MASTER;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SLEEPYCAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            