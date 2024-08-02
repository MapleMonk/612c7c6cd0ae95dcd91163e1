{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING_AMS AS with sessions as (select datastarttime, childasin, sum(trafficbyasin:sessions::float) as sessions, sum(trafficbyasin:pageViews::float) as pageviews from vahdam_db.maplemonk.asp_usa_get_sales_and_traffic_report_asin group by 1,2), orders as ( select \"Purchase-datetime-PDT\"::date as order_date, ASIN as asin, sum(ifnull(try_cast(\"item-price\" as float),0)) - sum(ifnull(try_cast(\"item-promotion-discount\" as float),0)) as sales, count(distinct \"amazon-order-id\") as orders, sum(QUANTITY) as quantity from (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'America/Los_Angeles\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-PDT\" FROM Vahdam_db.maplemonk.ASP_USA_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL)X where lower(\"order-status\") <> \'cancelled\' and lower(\"sales-channel\") = \'amazon.com\' group by 1,2 ), amazon as ( SELECT DATE::date DATE ,ASIN ASIN ,CAMPAIGN_TYPE CAMPAIGN_TYPE ,PROFILEID PROFILEID ,CAMPAIGN_ID CAMPAIGNID ,CAMPAIGNNAME CAMPAIGNNAME ,AD_GROUP_ID ADGROUPID ,ADGROUPNAME ADGROUPNAME ,KEYWORD_TEXT KEYWORDTEXT ,CAMPAIGNSTATUS CAMPAIGNSTATUS ,AD_ID ADID ,TARGETING_VALUE TARGETINGEXPRESSION ,TARGETING_TEXT TARGETINGTEXT ,CURRENCY CURRENCY ,SUM(IFNULL(ATTRIBUTED_ORDERS_NEW_TO_BRAND_14D,0)) NEWTOBRANDORDERS ,SUM(IFNULL(ATTRIBUTED_SALES_NEW_TO_BRAND_14D,0)) NEWTOBRANDSALES ,SUM(IFNULL(ATTRIBUTED_UNITS_ORDERED_NEW_TO_BRAND_14D,0)) NEWTOBRANDUNITS ,SUM(IFNULL(IMPRESSIONS,0)) IMPRESSIONS ,SUM(IFNULL(CLICKS,0)) CLICKS ,SUM(IFNULL(SPEND,0)) SPEND ,SUM(IFNULL(ATTRIBUTED_SALES_14D,0)) SALES ,SUM(IFNULL(ATTRIBUTED_CONVERSIONS_14D,0)) CONVERSIONS ,SUM(IFNULL(ATTRIBUTED_CONVERSIONS_14D_SAME_SKU,0)) CONVERSIONSSAMESKU ,SUM(IFNULL(ATTRIBUTED_SALES_14D_SAME_SKU,0)) SALESSAMESKU FROM VAHDAM_DB.maplemonk.AAMS_US_HOURLY_DATA_CONSOLIDATED where PROFILEID IN (\'1865822991259542\',\'4287412371422290\',\'2796474817192137\') GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14 ) select coalesce(c.date, s.datastarttime) as date ,coalesce(c.asin, s.childasin) as asin ,c.CAMPAIGN_TYPE ,c.PROFILEID ,c.CampaignID ,c.CampaignName ,c.AdGroupId ,c.AdGroupName ,c.KeywordText ,c.CampaignStatus ,c.AdId ,c.TargetingExpression , c.TargetingText , c.Currency , c.NewToBrandOrders , c.NewToBrandSales ,c.NewToBrandUnits , c.Impressions , c.Clicks , c.spend , c.Sales , c.Conversions , c.ConversionsSameSKU , c.SalesSameSKU ,c.OtherSKUSales , c.ConversionsOtherSKU , c.sales_usd , c.orders , c.quantity ,s.sessions/count(1)over(partition by coalesce(c.date, s.datastarttime), coalesce(c.asin, s.childasin)) as sessions ,s.pageviews/count(1)over(partition by coalesce(c.date, s.datastarttime), coalesce(c.asin, s.childasin)) as pageviews ,coalesce(b.\"Amazon USA\", c.asin ,s.childasin) as ASIN_New ,b.weight as Weight ,b.brand as Brand ,b.\"Mother SKU\" as Mother_SKU ,b.category as ProductCategory ,b.\"SUB CATEGORY\" as TypeOfTea ,b.\"LOOSE/TEA BAG/ POWDER\" as TypeOfProduct ,b.\"Common SKU Description\" as ProductName ,b.\"COMMON SKU ID\" as CommonSKU_Id ,Null as TypeOfPack from (select coalesce(a.date, o.order_date::date) as date ,coalesce(a.asin, o.asin) as asin ,a.CAMPAIGN_TYPE , a.PROFILEID , a.CampaignID , a.CampaignName , a.AdGroupId , a.AdGroupName , a.KeywordText , a.CampaignStatus , a.AdId , a.TargetingExpression , a.TargetingText , a.Currency , a.NewToBrandOrders , a.NewToBrandSales , a.NewToBrandUnits , a.Impressions , a.Clicks , a.spend , a.Sales , a.Conversions , a.ConversionsSameSKU , a.SalesSameSKU , a.Sales-a.SalesSameSKU AS OtherSKUSales , a.Conversions-a.ConversionsSameSKU AS ConversionsOtherSKU ,o.Sales/count(1)over(partition by coalesce(a.date, o.order_date::date),coalesce(a.asin, o.asin)) as Sales_usd ,o.orders/count(1)over(partition by coalesce(a.date, o.order_date::date),coalesce(a.asin, o.asin)) as orders ,o.quantity/count(1)over(partition by coalesce(a.date, o.order_date::date),coalesce(a.asin, o.asin)) as quantity from amazon a full outer join orders o on a.asin = o.asin and order_date::date = a.Date::date ) c full outer join sessions s on c.asin = s.childasin and c.date = s.datastarttime LEFT JOIN (select * from (select \"Amazon USA\" ,weight ,brand ,\"Mother SKU\" ,\"Common Name\" ,category ,\"SUB CATEGORY\" ,\"LOOSE/TEA BAG/ POWDER\" ,\"Common SKU Description\" ,\"COMMON SKU ID\" ,row_number() over (partition by \"Amazon USA\" order by \"Amazon USA\") as rw from vahdam_db.maplemonk.sku_mapping_raw_data) where rw = 1) b on coalesce(c.asin, s.childasin) = b.\"Amazon USA\" ; create or replace table VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING as select DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,NULL AS ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING_AMS WHERE DATE >= \'2024-07-21\' UNION ALL select DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING_PV3 WHERE DATE >= \'2023-10-01\' AND DATE < \'2024-07-21\' union all select DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING_PV2_Consol WHERE DATE < \'2023-10-01\';",
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
            