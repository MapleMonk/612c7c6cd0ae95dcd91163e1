{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table connectyai_db.connectyai.Pharma_Sample_Sales_Fact_Items as select Upper(COUNTRY) COUNTRY ,Upper(CLASS) CLASS ,upper(molecule) MOLECULE ,upper(\"Type of Drug\") Type_OF_DRUG ,YEAR ,try_cast(SALES as float) SALES_IN_USD from connectyai_db.connectyai.pharma_sample_sales_in_mm_usd UNPIVOT(sales FOR Year IN (\"2017\",\"2018\",\"2019\",\"2020\",\"2021\",\"2022\",\"2023\",\"2024\",\"2025\",\"2026\",\"2027\")) order by year, country, CLASS, MOLECULE, \"Type of Drug\";",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from CONNECTYAI_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        