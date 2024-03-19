{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_SAAS_cost as with B2C_base as ( select \"MP Name\" as marketplace, \"Reference Code\" as reference_code, \"Order Type\" as order_type, try_cast(\"Item Quantity\" as float) as quantity, \"Suborder No\" as Suborder_No from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_sales where lower(\"Order Type\") = \'b2c\' and \"Reference Code\" not like \'%_DEL_%\' ), B2B_base as ( select \"MP Name\" as marketplace, \"Reference Code\" as reference_code, \"Order Type\" as order_type, try_cast(\"Item Quantity\" as float) as quantity, \"Suborder No\" as Suborder_No from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_sales where lower(\"Order Type\") in (\'b2b\', \'stn\') and \"Reference Code\" not like \'%_DEL_%\' ), Other_B2C_base as ( select o.reference_code, o.marketplace, case when (tot_quantity / 3) < 1 then 4 else (ceil(tot_quantity / 3)*4) end as Easyecom_cost from ( select reference_code, marketplace, sum(quantity)as tot_quantity from B2C_base where lower(marketplace) not like \'%flipkart%\' group by 1,2 )as o ), Flipkart_B2C_base as ( select o.reference_code, o.marketplace, sum(case when (tot_quantity / 3) < 1 then 4 else (ceil(tot_quantity / 3)*4) end) as Easyecom_cost from ( select reference_code, Suborder_No, marketplace, sum(quantity)as tot_quantity from B2C_base where lower(marketplace) like \'%flipkart%\' group by 1,2,3 )as o group by 1,2 ), B2B_easyecom_base as ( select o.reference_code, o.marketplace, case when (tot_quantity / 20) < 1 then 4 else (ceil(tot_quantity / 20)*4) end as Easyecom_cost from ( select reference_code, marketplace, sum(quantity)as tot_quantity from B2B_base group by 1,2 )as o ), Interakt_base as ( select marketplace, reference_code, sum(case when report_date <= date(\'2023-12-31\') then (0.45*4) else (0.35*4) end) as Interakt_cost from ( select distinct \"MP Name\" as marketplace, \"Reference Code\" as reference_code, date(try_cast(\"Invoice Date\" as timestamp)) as report_date from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_sales where lower(\"MP Name\") like \'%shopify%\' and \"Reference Code\" not like \'%_DEL_%\' )as o group by 1,2 ), ClickPost_base as ( select marketplace, reference_code, round(div0(5000, count(1) over (partition by month_Date order by 1)),2) as Clickpost_cost from ( select distinct \"MP Name\" as marketplace, \"Reference Code\" as reference_code, date(date_trunc(\'month\', try_cast(\"Invoice Date\" as timestamp))) as month_Date from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_sales where lower(\"MP Name\") like \'%shopify%\' and lower(\"Order Status\") not like \'%cancel%\' and \"Reference Code\" not like \'%_DEL_%\' )as o ), final_base_EE as ( select distinct o.* , date(date_trunc(\'month\', try_cast(\"Invoice Date\" as timestamp))) as month_Date from ( select * from Other_B2C_base union select * from Flipkart_B2C_base union select * from B2B_easyecom_base )as o inner join HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_sales a on o.reference_code = a.\"Reference Code\" ), EE_base_data as ( select reference_code, marketplace, sum(EE_cost)as Easyecom_cost from ( select a.reference_code, a.marketplace, a.month_Date, round(div0(o.Easyecom_cost, count(1) over (partition by a.month_Date order by 1)),2) as EE_cost from final_base_EE a left join ( select distinct o.month_Date, case when EE_cost <= 25000 then 25000 else EE_cost end as Easyecom_cost from ( select month_Date , sum(Easyecom_cost)as EE_cost from final_base_EE group by 1 order by 1 )as o )as o on a.month_Date = o.month_Date )as o group by 1,2 ) select distinct o.*, a.Interakt_cost, b.Clickpost_cost from EE_base_data o left join Interakt_base a on o.REFERENCE_CODE = a.REFERENCE_CODE left join ClickPost_base b on o.REFERENCE_CODE = b.REFERENCE_CODE",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        