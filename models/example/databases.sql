{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table connectyai_db.connectyai.Pharma_Sample_Price_Per_Treated_Day_in_USD_Fact_Items as Select * from ( select upper(COUNTRY) COUNTRY ,Upper(CLASS) CLASS ,upper(molecule) MOLECULE ,upper(\"Type of Drug\") Type_OF_DRUG ,YEAR ,try_cast(Price_Per_Treated_Day_in_USD as float) Price_Per_Treated_Day_in_USD ,row_number() over (partition by COUNTRY, CLASS, MOLECULE, \"Type of Drug\", YEAR order by 1) rw from connectyai_db.connectyai.PHARMA_SAMPLE_PRICE_PER_TREATED_DAY_IN_USD UNPIVOT(Price_Per_Treated_Day_in_USD FOR Year IN (\"2017\",\"2018\",\"2019\",\"2020\",\"2021\",\"2022\",\"2023\",\"2024\",\"2025\",\"2026\",\"2027\")) order by year, country, CLASS, MOLECULE, \"Type of Drug\" ) where rw = 1;",
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
                        