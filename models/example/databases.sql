{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.amazon1pads_FR_marketing as With Orders as ( select startdate::date as Date ,ASIN as ASIN ,sum(SHIPPEDCOGS:amount::float) as SHIPPEDCOGS_Amount ,sum(ORDEREDREVENUE:amount::float) as ORDEREDREVENUE_Amount ,sum(SHIPPEDREVENUE:amount::float) as SHIPPEDREVENUE_Amount ,sum(orderedunits) as Ordered_units ,sum(shippedunits) as Shipped_units from vahdam_db.maplemonk.avp_man_fr_get_vendor_sales_report group by 1,2 ), Primaryorders as ( select startdate::date as Date ,ASIN as ASIN ,sum(SHIPPEDCOGS:amount::float) as Primary_SHIPPEDCOGS_Amount ,sum(ORDEREDREVENUE:amount::float) as Primary_ORDEREDREVENUE_Amount ,sum(SHIPPEDREVENUE:amount::float) as Primary_SHIPPEDREVENUE_Amount ,sum(orderedunits) as Primary_Ordered_units ,sum(shippedunits) as Primary_Shipped_units from vahdam_db.maplemonk.avp_fr_get_vendor_sales_report group by 1,2 ), Sessions as ( select startdate::date as Date, asin as ASIN, sum(glanceviews) as Sessions from vahdam_db.maplemonk.avp_man_fr_get_vendor_traffic_report group by 1,2 ), Mapping as ( select * from (select \"Amazon EU 1P\" ,weight ,brand ,\"Mother SKU\" ,\"Common Name\" ,category ,\"SUB CATEGORY\" ,\"LOOSE/TEA BAG/ POWDER\" ,\"Common SKU Description\" ,\"COMMON SKU ID\" ,row_number() over (partition by \"Amazon EU 1P\" order by \"Amazon EU 1P\") as rw from vahdam_db.maplemonk.sku_mapping_raw_data) where rw = 1 or rw is null ) select coalesce(O.Date, S.Date,P.Date) as Date ,coalesce(O.ASIN, S.ASIN,P.ASIN,M.\"Amazon EU 1P\") as ASIN ,O.SHIPPEDCOGS_Amount ,O.ORDEREDREVENUE_Amount ,O.SHIPPEDREVENUE_Amount ,O.Ordered_units ,O.Shipped_units ,P.Primary_SHIPPEDCOGS_Amount ,P.Primary_ORDEREDREVENUE_Amount ,P.Primary_SHIPPEDREVENUE_Amount ,P.Primary_Ordered_units ,P.Primary_Shipped_units ,S.Sessions ,M.weight ,M.brand ,M.\"Mother SKU\" ,M.\"Common Name\" ,M.category ,M.\"SUB CATEGORY\" ,M.\"LOOSE/TEA BAG/ POWDER\" ,M.\"Common SKU Description\" ,M.\"COMMON SKU ID\" from Orders O full outer join SESSIONS S on O.Date=S.Date and O.ASIN=S.ASIN full outer join primaryorders P on O.Date=P.Date and O.ASIN=P.ASIN left join MAPPING M on coalesce(O.ASIN, S.ASIN,P.ASIN) = M.\"Amazon EU 1P\" order by 1 desc;",
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
                        