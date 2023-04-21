{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.Amazon_USA_Budget_Comparison_Vahdam as with Budget as (Select try_to_date(month) as Budget_month ,asin as ASIN ,\"Product Description\" as Product_Name ,sum(cast(\"Per Units COGS\" as float)) as Per_unit_Cogs ,sum(cast(\"Per Unit Outbound\" as float)) as Per_unit_Outbound ,sum(cast(units as float)) as Budget_units ,sum(cast(ASP as float)) as Budget_ASP ,sum(cast(Revenue as float)) as Budget_revenue ,sum(cast(COGS as float)) as Budget_COGS ,sum(cast(\"Comm \" as float)) as Budget_Commission ,sum(cast(\"Outbound \" as float)) as Budget_Outbound ,sum(cast(\"Storage \" as float)) as Budget_Storage ,sum(cast(\"Last Mile \" as float)) as Budget_FBA ,sum(cast(\"PM SPEND\" as float)) as Budget_PM_Spend ,sum(cast(Revenue as float))-sum(cast(COGS as float))-sum(cast(\"Comm \" as float))-sum(cast(\"Outbound \" as float))-sum(cast(\"Storage \" as float))-sum(cast(\"Last Mile \" as float))-sum(cast(\"PM SPEND\" as float)) as Budget_CM2 ,sum(cast(Revenue as float))-sum(cast(COGS as float))-sum(cast(\"Comm \" as float))-sum(cast(\"Outbound \" as float))-sum(cast(\"Storage \" as float))-sum(cast(\"Last Mile \" as float)) as Budget_CM1 from vahdam_db.maplemonk.gs_fy24_budget_consol_backup where lower(channel) = \'amazon\' and lower(platform) = \'usa\' group by 1,2,3), Payments as (select *,sum(units) over (partition by asin,month(date),year(date) order by year(date), month(date), date) as MTD_units ,sum(sales) over (partition by asin,month(date),year(date) order by year(date), month(date), date) as MTD_sales from (select DATE ,ASIN ,PRODUCTNAME ,SKU_NEW ,COMMON_SKU ,sum(ifnull(PRINCIPAL,0))+sum(ifnull(PROMOTION,0))+sum(ifnull(TAX,0))+sum(ifnull(REFUND_TAX,0))+sum(ifnull(REFUND_PRINCIPAL,0))+sum(ifnull(REFUND_SHIPPINGTAX,0))+sum(ifnull(REFUND_SHIPPINGCHARGE,0))+sum(ifnull(REFUND_GIFTWRAP,0))+sum(ifnull(REFUND_GIFTWRAPTAX,0))+sum(ifnull(REFUND_GIFTWRAPCHARGEBACK,0))+sum(ifnull(REFUND_SHIPPINGCHARGEBACK,0))+sum(ifnull(REFUND_MARKETPLACEFACILITATORTAX_PRINCIPAL,0))+sum(ifnull(REFUND_MARKETPLACEFACILITATORTAX_SHIPPING,0))+sum(ifnull(REFUND_MARKETPLACEFACILITATORTAX_OTHER,0))+sum(ifnull(GIFTWRAP,0))+sum(ifnull(GIFTWRAPTAX,0))+sum(ifnull(SHIPPINGCHARGE,0))+sum(ifnull(SHIPPINGTAX,0))+sum(ifnull(MARKETPLACEFACILITATORTAXPRINCIPAL,0))+sum(ifnull(MARKETPLACEFACILITATORTAXSHIPPING,0))+sum(ifnull(MARKETPLACEFACILITATORTAXOTHER,0))+sum(ifnull(GIFTWRAPCHARGEBACK,0))+sum(ifnull(SHIPPINGCHARGEBACK,0))+sum(ifnull(REFUND_PROMOTION,0))+sum(ifnull(marketplacefacilitatorvatshipping,0))+sum(ifnull(marketplacefacilitatorvatprincipal,0))+sum(ifnull(refund_marketplacefacilitatorvat_shipping,0))+sum(ifnull(refund_marketplacefacilitatorvat_principal,0)) as Sales ,sum(ifnull(quantity,0))-sum(ifnull(refund_quantity,0)) as Units ,sum(ifnull(quantity*cogs_usd,0)) as COGS_USD ,sum(ifnull(quantity*outbound_cost_usd,0)) as Outbound_Cost ,-1*(sum(ifnull(Commission,0))+sum(ifnull(REFUND_COMMISSION,0))+sum(ifnull(REFUND_REFUNDCOMMISSION,0))) as Commission ,sum(ifnull(quantity*storage_cost,0)) as Storage_Cost ,-1*(sum(ifnull(fbaperunitfulfillmentfee,0))) as FBA_Fees ,sum(ifnull(total_amazon_spend,0)) as Total_Marketing_Spend from vahdam_db.maplemonk.asp_us_asin_payment_reports group by 1,2,3,4,5)) select coalesce(p.date,b.budget_month) as Date ,coalesce(date_trunc(\'month\',p.date),date_trunc(\'month\',b.budget_month)) as Month_start ,coalesce(monthname(p.date),monthname(b.budget_month)) as Month ,coalesce(p.asin,b.asin) as ASIN ,coalesce(p.PRODUCTNAME,b.Product_Name) as Product_Name ,P.MTD_units ,P.MTD_sales ,P.Sales ,P.Units ,P.COGS_USD ,P.Outbound_Cost ,P.Commission ,P.Storage_Cost ,P.FBA_Fees ,P.Total_Marketing_Spend ,b.Budget_units ,b.Budget_ASP ,b.Budget_revenue ,b.Budget_COGS ,b.Budget_Commission ,b.Budget_Outbound ,b.Budget_Storage ,b.Budget_FBA ,b.Budget_PM_Spend ,b.Budget_CM2 ,b.Budget_units*dayofmonth(coalesce(p.date,b.budget_month))/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as MTD_Budget_Units ,b.Budget_units/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Units ,b.Budget_Revenue/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Revenue ,b.Budget_COGS/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_COGS ,b.Budget_Commission/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Commission ,b.Budget_Outbound/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Outbound ,b.Budget_Storage/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Storage ,b.Budget_FBA/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_FBA ,b.Budget_PM_Spend/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_PM_Spend ,b.Budget_CM2/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_CM2 ,b.Budget_CM1/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_CM1 from payments p full outer join budget b on date_trunc(\'month\',p.date) = date_trunc(\'month\',b.budget_month) and p.asin = b.asin order by 1 desc",
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
                        