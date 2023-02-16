{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.AMAZON_US_INVENTORY_SALES_SPEND_VAHDAM as With Amazonads as (select date ,asin ,sum(ifnull(sales_usd,0)) as Total_sales ,sum(ifnull(quantity,0)) as Units ,sum(ifnull(spend,0)) as Total_spend ,sum(sum(ifnull(quantity,0))) over (partition by ASIN order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as last_7days_sales ,sum(sum(ifnull(quantity,0))) over (partition by ASIN order by date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as last_30days_sales from vahdam_db.maplemonk.amazonads_na_marketing group by 1,2) , Inventory as (select ASIN ,sum(ifnull(inbound,0)) as Inbound_inventory ,sum(ifnull(shipped,0)) as Shipped_inventory ,sum(ifnull(working,0)) as Working_inventory ,sum(ifnull(receiving,0)) as Receiving_inventory ,sum(ifnull(Available,0)) as Available_inventory ,sum(ifnull(\"FC transfer\",0)) as FC_Transfer_inventory ,sum(ifnull(\"FC Processing\",0)) as FC_Processing_inventory from vahdam_db.maplemonk.casp_usa_get_restock_inventory_recommendations_report group by 1) select date ,coalesce(a.asin,I.ASIN) as ASIN ,A.Total_sales as Total_sales ,A.Units as Units ,A.Total_spend as Total_spend ,A.last_7days_sales as last_7days_sales ,A.last_30days_sales as last_30days_sales ,I.Inbound_inventory as Current_Inbound_inventory ,I.Shipped_inventory as Current_Shipped_inventory ,I.Working_inventory as Current_Working_inventory ,I.Receiving_inventory as Current_Receiving_inventory ,I.Available_inventory as Current_Available_inventory ,I.FC_Transfer_inventory as Current_FC_Transfer_inventory ,I.FC_Processing_inventory as Current_FC_Processing_inventory From AMAZONADS A left join Inventory I on a.asin = I.ASIN order by date desc",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        