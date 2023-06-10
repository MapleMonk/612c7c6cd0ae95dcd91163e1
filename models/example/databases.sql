{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table vahdam_db.maplemonk.VAHDAM_ASP_UK_REFUND_SHIPMENT_ORDER_ID as With Refund as (Select POSTEDDATE::date as POSTEDDATE ,AMAZONORDERID ,SELLERSKU ,sum(ifnull(Tax,0)) Refund_Tax ,sum(ifnull(Principal,0)) Refund_Principal ,sum(ifnull(Refund_Promotion,0)) Refund_Promotion ,sum(ifnull(ShippingTax,0)) Refund_ShippingTax ,sum(ifnull(ShippingCharge,0)) Refund_ShippingCharge ,sum(ifnull(GiftWrap,0)) Refund_GiftWrap ,sum(ifnull(GiftWrapTax,0)) Refund_GiftWrapTax ,sum(ifnull(ExportCharge,0)) Refund_ExportCharge ,sum(ifnull(ReturnShipping,0)) Refund_ReturnShipping ,sum(ifnull(GenericDeduction,0)) Refund_GenericDeduction ,sum(ifnull(Goodwill,0)) Refund_Goodwill ,sum(ifnull(Commission,0)) Refund_Commission ,sum(ifnull(RefundCommission,0)) Refund_RefundCommission ,sum(ifnull(GiftwrapChargeback,0)) Refund_GiftwrapChargeback ,sum(ifnull(ShippingChargeback,0)) Refund_ShippingChargeback ,sum(ifnull(Quantity,0)) Refund_Quantity ,sum(ifnull(Refund_MarketplaceFacilitatorTax_Other,0)) as Refund_MarketplaceFacilitatorTax_Other ,sum(ifnull(Refund_MarketplaceFacilitatorVat_Shipping,0)) as Refund_MarketplaceFacilitatorVat_Shipping ,sum(ifnull(Refund_MarketplaceFacilitatorVat_Principal,0)) as Refund_MarketplaceFacilitatorVat_Principal from vahdam_db.maplemonk.vahdam_UK_refund_event_list group by 1,2,3), Orders as ( select * from (select SKU , ASIN , row_number() over (partition by SKU order by ASIN) rw from ( select distinct SKU, ASIN from vahdam_db.maplemonk.ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL where year(\"purchase-date\"::date) >= \'2023\' and lower(\"sales-channel\") = \'amazon.co.uk\' and sku is not null ) ) where rw=1 ), Shipment as (select POSTEDDATE::date as POSTEDDATE ,AMAZONORDERID ,SELLERSKU ,MarketplaceName ,sum(QUANTITY) QUANTITY ,sum(PRINCIPAL) PRINCIPAL ,sum(FBAPERUNITFULFILLMENTFEE) FBAPERUNITFULFILLMENTFEE ,sum(COMMISSION) COMMISSION ,sum(PROMOTION) PROMOTION ,sum(TAX) TAX ,sum(GIFTWRAP) GIFTWRAP ,sum(GIFTWRAPTAX) GIFTWRAPTAX ,sum(SHIPPINGCHARGE) SHIPPINGCHARGE ,sum(SHIPPINGTAX) SHIPPINGTAX ,sum(MARKETPLACEFACILITATORTAXOTHER) MARKETPLACEFACILITATORTAXOTHER ,sum(MARKETPLACEFACILITATORVATSHIPPING) MARKETPLACEFACILITATORVATSHIPPING ,sum(MARKETPLACEFACILITATORVATPRINCIPAL) MARKETPLACEFACILITATORVATPRINCIPAL ,sum(FIXEDCLOSINGFEE) FIXEDCLOSINGFEE ,sum(GIFTWRAPCHARGEBACK) GIFTWRAPCHARGEBACK ,sum(SHIPPINGCHARGEBACK) SHIPPINGCHARGEBACK ,sum(VARIABLECLOSINGFEE) VARIABLECLOSINGFEE from vahdam_db.maplemonk.vahdam_UK_shipment_event_list group by 1,2,3,4) select coalesce(S.POSTEDDATE,R.POSTEDDATE)::date POSTEDDATE ,coalesce(S.AMAZONORDERID,R.AMAZONORDERID) AMAZONORDERID ,coalesce(S.SELLERSKU,R.SELLERSKU) SELLERSKU ,O.ASIN as ASIN ,S.MarketplaceName MarketplaceName ,S.QUANTITY ,S.PRINCIPAL ,S.FBAPERUNITFULFILLMENTFEE ,S.COMMISSION ,S.PROMOTION ,S.TAX ,ifnull(R.Refund_Tax,0) Refund_Tax ,ifnull(R.Refund_Principal,0) Refund_Principal ,ifnull(R.Refund_Promotion,0) Refund_Promotion ,ifnull(R.Refund_ShippingTax,0) Refund_ShippingTax ,ifnull(R.Refund_ShippingCharge,0) Refund_ShippingCharge ,ifnull(R.Refund_GiftWrap,0) Refund_GiftWrap ,ifnull(R.Refund_GiftWrapTax,0) Refund_GiftWrapTax ,ifnull(R.Refund_ExportCharge,0) Refund_ExportCharge ,ifnull(R.Refund_ReturnShipping,0) Refund_ReturnShipping ,ifnull(R.Refund_GenericDeduction,0) Refund_GenericDeduction ,ifnull(R.Refund_Goodwill,0) Refund_Goodwill ,ifnull(R.Refund_Commission,0) Refund_Commission ,ifnull(R.Refund_RefundCommission,0) Refund_RefundCommission ,ifnull(R.Refund_GiftwrapChargeback,0) Refund_GiftwrapChargeback ,ifnull(R.Refund_ShippingChargeback,0) Refund_ShippingChargeback ,ifnull(R.Refund_Quantity,0) Refund_Quantity ,ifnull(R.Refund_MarketplaceFacilitatorTax_Other,0) as Refund_MarketplaceFacilitatorTax_Other ,ifnull(R.Refund_MarketplaceFacilitatorVat_Shipping,0) as Refund_MarketplaceFacilitatorVat_Shipping ,ifnull(R.Refund_MarketplaceFacilitatorVat_Principal,0)as Refund_MarketplaceFacilitatorVat_Principal ,S.GIFTWRAP ,S.GIFTWRAPTAX ,S.SHIPPINGCHARGE ,S.SHIPPINGTAX ,S.MARKETPLACEFACILITATORTAXOTHER ,S.MARKETPLACEFACILITATORVATSHIPPING ,S.MARKETPLACEFACILITATORVATPRINCIPAL ,S.FIXEDCLOSINGFEE ,S.GIFTWRAPCHARGEBACK ,S.SHIPPINGCHARGEBACK ,S.VARIABLECLOSINGFEE from Shipment S full outer join Refund R on S.AMAZONORDERID = R.AMAZONORDERID and lower(S.SELLERSKU) = lower(R.SELLERSKU) left join Orders O on lower(S.SELLERSKU) = lower(O.SKU) where lower(MarketplaceName) = \'amazon.co.uk\' order by POSTEDDATE desc; Create or replace table vahdam_db.maplemonk.ASP_UK_ASIN_Payment_Reports_Budget_Tracking as with Payments as ( select POSTEDDATE::date as Date ,SELLERSKU ,ASIN ,MarketplaceName ,sum(ifnull(PRINCIPAL,0))+sum(ifnull(PROMOTION,0))+sum(ifnull(TAX,0))+sum(ifnull(REFUND_TAX,0))+sum(ifnull(REFUND_PRINCIPAL,0))+sum(ifnull(REFUND_PROMOTION,0))+sum(ifnull(REFUND_SHIPPINGTAX,0))+sum(ifnull(REFUND_SHIPPINGCHARGE,0))+sum(ifnull(REFUND_GIFTWRAP,0))+sum(ifnull(REFUND_GIFTWRAPTAX,0))+sum(ifnull(REFUND_EXPORTCHARGE,0))+sum(ifnull(REFUND_RETURNSHIPPING,0))+sum(ifnull(REFUND_GENERICDEDUCTION,0))+sum(ifnull(REFUND_GOODWILL,0))+sum(ifnull(REFUND_GIFTWRAPCHARGEBACK,0))+sum(ifnull(REFUND_SHIPPINGCHARGEBACK,0))+sum(ifnull(REFUND_MARKETPLACEFACILITATORTAX_OTHER,0))+sum(ifnull(REFUND_MARKETPLACEFACILITATORVAT_SHIPPING,0))+sum(ifnull(REFUND_MARKETPLACEFACILITATORVAT_PRINCIPAL,0))+sum(ifnull(GIFTWRAP,0))+sum(ifnull(GIFTWRAPTAX,0))+sum(ifnull(SHIPPINGCHARGE,0))+sum(ifnull(SHIPPINGTAX,0))+sum(ifnull(MARKETPLACEFACILITATORTAXOTHER,0))+sum(ifnull(MARKETPLACEFACILITATORVATSHIPPING,0))+sum(ifnull(MARKETPLACEFACILITATORVATPRINCIPAL,0))+sum(ifnull(FIXEDCLOSINGFEE,0))+sum(ifnull(GIFTWRAPCHARGEBACK,0))+sum(ifnull(SHIPPINGCHARGEBACK,0))+sum(ifnull(VARIABLECLOSINGFEE,0)) as Sales ,-1*sum(ifnull(FBAPERUNITFULFILLMENTFEE,0)) as FBA_Fees ,-1*(sum(ifnull(Commission,0))+sum(ifnull(REFUND_COMMISSION,0))+sum(ifnull(REFUND_REFUNDCOMMISSION,0))) as Commission ,sum(ifnull(quantity,0)) as Quantity_sold ,sum(ifnull(refund_quantity,0)) Refund_Quanity ,sum(ifnull(quantity,0))-sum(ifnull(refund_quantity,0)) as Units from vahdam_db.maplemonk.VAHDAM_ASP_UK_REFUND_SHIPMENT_ORDER_ID group by 1,2,3,4 ), Budget as ( Select Budget_month ,ASIN ,sum(Per_unit_Cogs) as Per_unit_Cogs ,sum(Per_unit_Outbound) as Per_unit_Outbound ,sum(Budget_units) as Budget_units ,sum(ASP) as Budget_ASP ,sum(Budget_revenue) as Budget_revenue ,sum(Budget_COGS) as Budget_COGS ,sum(Budget_Commission) as Budget_Commission ,sum(Budget_Outbound) as Budget_Outbound ,sum(Budget_Storage) as Budget_Storage ,sum(Budget_Last_mile) as Budget_FBA ,sum(Budget_PM_Spend) as Budget_PM_Spend ,sum(Budget_revenue)-sum(Budget_COGS)-sum(Budget_Commission)-sum(Budget_Outbound)-sum(Budget_Storage)-sum(Budget_Last_mile)-sum(Budget_PM_Spend) as Budget_CM2 ,sum(Budget_revenue)-sum(Budget_COGS)-sum(Budget_Commission)-sum(Budget_Outbound)-sum(Budget_Storage)-sum(Budget_Last_mile) as Budget_CM1 ,sum(ifnull(Budget_Revenue_INR,0)) Budget_Revenue_INR ,sum(ifnull(Budget_COGS_INR,0)) Budget_COGS_INR ,sum(ifnull(Budget_Commission_INR,0)) Budget_Commission_INR ,sum(ifnull(Budget_Outbound_INR,0)) Budget_Outbound_INR ,sum(ifnull(Budget_Storage_INR,0)) Budget_Storage_INR ,sum(ifnull(Budget_FBA_INR,0)) Budget_FBA_INR ,sum(ifnull(Budget_CM1_INR,0)) Budget_CM1_INR ,sum(ifnull(Budget_PM_SPEND_INR,0)) Budget_PM_SPEND_INR ,sum(ifnull(Budget_CM2_INR,0)) Budget_CM2_INR ,row_number() over (partition by Budget_month,asin order by sum(Budget_units) desc) rw from (select try_to_date(month) as Budget_Month ,asin as ASIN ,cast(\"Per Units COGS\" as float) as Per_Unit_COGS ,cast(\"Per Unit Outbound\" as float) as Per_unit_outbound ,cast(units as float) as Budget_units ,cast(ASP as float) as ASP ,cast(Revenue as float) as Budget_revenue ,cast(COGS as float) as Budget_COGS ,cast(\"Comm \" as float) as Budget_Commission ,cast(\"Outbound \" as float) as Budget_Outbound ,cast(\"Storage \" as float) as Budget_Storage ,cast(\"Last Mile \" as float) as Budget_Last_mile ,cast(\"PM SPEND\" as float) as Budget_PM_Spend ,cast(\"Revenue(INR)\" as float) as Budget_Revenue_INR ,cast(\"REVISED COGS\" as float) as Budget_COGS_INR ,cast(\"Comm (INR)\" as float) as Budget_Commission_INR ,cast(\"Outbound(INR)\" as float) as Budget_Outbound_INR ,cast(\"Storage(INR)\" as float) as Budget_Storage_INR ,cast(\"Last Mile (INR)\" as float) as Budget_FBA_INR ,cast(\"CM1 (INR)\" as float) as Budget_CM1_INR ,cast(\"PM SPEND (INR)\" as float) as Budget_PM_SPEND_INR ,cast(\"CM2\" as float) as Budget_CM2_INR ,row_number() over (partition by month,asin order by cast(units as float) desc) rw from vahdam_db.maplemonk.gs_fy24_budget_consol_backup where lower(channel) = \'amazon\' and lower(platform) = \'uk\') where rw = 1 group by 1,2 ), Amazonads as (select date ,case when asin_new is null then \'SB\' else asin_new end as ASIN ,sum(ifnull(spend,0)) as Total_amazon_spend ,sum(ifnull(sales_usd,0)) as Total_amazon_sales from vahdam_db.maplemonk.amazonads_uk_marketing group by 1,2 order by 1 desc), Mapping as ( select * from (select \"Amazon UK\" ,weight ,brand ,\"Mother SKU\" ,\"Common Name\" ,category ,\"SUB CATEGORY\" ,\"LOOSE/TEA BAG/ POWDER\" ,\"Common SKU Description\" ,\"COMMON SKU ID\" ,row_number() over (partition by \"Amazon UK\" order by \"Amazon UK\") as rw from vahdam_db.maplemonk.sku_mapping_raw_data) where rw = 1 or rw is null ) select coalesce(P.Date,A.date,B.Budget_month) as Date ,coalesce(P.ASIN,A.ASIN,B.ASIN,M.\"Amazon UK\") as ASIN ,ifnull(P.Sales,0) as Sales ,ifnull(P.FBA_Fees,0) as FBA_Fees ,ifnull(P.Commission,0) as Commission ,ifnull(P.Quantity_sold,0) Quantity_sold ,ifnull(P.Refund_Quanity,0) Refund_Quanity ,ifnull(P.Units,0) as Units ,ifnull(A.Total_amazon_spend,0) as Total_amazon_spend ,B.Per_unit_Cogs ,B.Per_unit_Outbound ,B.Budget_ASP ,B.Budget_units/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Units ,B.Budget_Revenue/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Revenue ,B.Budget_COGS/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_COGS ,B.Budget_Commission/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Commission ,B.Budget_Outbound/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Outbound ,B.Budget_Storage/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Storage ,B.Budget_FBA/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_FBA ,B.Budget_PM_Spend/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_PM_Spend ,B.Budget_CM2/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_CM2 ,B.Budget_CM1/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_CM1 ,B.Budget_Revenue_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Revenue_INR ,B.Budget_COGS_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_COGS_INR ,B.Budget_Commission_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Commission_INR ,B.Budget_Outbound_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Outbound_INR ,B.Budget_Storage_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_Storage_INR ,B.Budget_FBA_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_FBA_INR ,B.Budget_CM1_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_CM1_INR ,B.Budget_PM_SPEND_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_PM_SPEND_INR ,B.Budget_CM2_INR/dayofmonth(last_day(coalesce(p.date,b.budget_month))) as Daily_Budget_CM2_INR ,M.weight ,M.brand ,M.\"Mother SKU\" ,M.\"Common Name\" ,M.category ,M.\"SUB CATEGORY\" ,M.\"LOOSE/TEA BAG/ POWDER\" ,M.\"Common SKU Description\" ,M.\"COMMON SKU ID\" from payments P full outer join AMAZONADS A on P.Date = A.date and P.ASIN = A.ASIN full outer join Budget B on coalesce(date_trunc(\'month\',P.Date),date_trunc(\'month\',A.Date)) = date_trunc(\'month\',B.Budget_month) and coalesce(P.ASIN,A.ASIN) = B.ASIN left join MAPPING M on lower(coalesce(P.ASIN,B.ASIN,A.ASIN)) = lower(M.\"Amazon UK\") order by Date desc;",
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
                        