{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table connectyai_db.connectyai.Pharma_Sample_Compliance_Fact_Items as select upper(COUNTRY) COUNTRY ,Upper(CLASS) CLASS ,upper(molecule) MOLECULE ,upper(\"Type of Drug\") Type_OF_DRUG ,YEAR ,try_cast(replace(Compliance,\'%\',\'\') as float)/100 Compliance from connectyai_db.connectyai.PHARMA_SAMPLE_COMPLIANCE UNPIVOT(Compliance FOR Year IN (\"2017\",\"2018\",\"2019\",\"2020\",\"2021\",\"2022\",\"2023\",\"2024\",\"2025\",\"2026\",\"2027\")) order by year, country, CLASS, MOLECULE, \"Type of Drug\";",
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
                        