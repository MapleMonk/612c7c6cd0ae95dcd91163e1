{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table rpsg_db.maplemonk.target_drv_three60_day_line_Chart as SELECT Date, ROUND(SUM(case when CATEGORY_IDENTIFIER in (\'DRV_Other_Marketplaces\') then booked_revenue_after_tax else 0 END),2) DRV_Ecom_Booked_Revenue, ROUND(SUM(case when CATEGORY_IDENTIFIER in (\'DRV_D2C\') then booked_revenue_after_tax else 0 END),2) DRV_D2C_Booked_Revenue, ROUND(SUM(case when CATEGORY_IDENTIFIER in (\'DRV_D2C\',\'DRV_Other_Marketplaces\') then booked_revenue_after_tax else 0 END),2)DRV_Total_Booked_Revenue, ROUND(SUM(case when CATEGORY_IDENTIFIER in (\'three60_Other_Marketplaces\') then booked_revenue_after_tax else 0 END),2)Three60_Ecom_Booked_Revenue, ROUND(SUM(case when CATEGORY_IDENTIFIER in (\'THREE60_D2C\') then booked_revenue_after_tax else 0 END),2) Three60_D2C_Booked_Revenue, ROUND(SUM(case when CATEGORY_IDENTIFIER in (\'THREE60_D2C\',\'three60_Other_Marketplaces\') then booked_revenue_after_tax else 0 END),2) Three60_Total_Booked_Revenue, FROM rpsg_db.maplemonk.target_drv_three60_day WHERE DATE BETWEEN CURRENT_DATE()-16 AND CURRENT_DATE()-1 group by 1 ORDER BY 1 DESC;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            