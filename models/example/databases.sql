{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table VAHDAM_DB.MAPLEMONK.VAHDAM_DB_AMAZONADS_MARKETING_CONSOLIDATED as select \'USA\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_NA_MARKETING union all select \'CA\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_CA_MARKETING UNION ALL select \'MX\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_MX_MARKETING UNION ALL select \'UK\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_UK_MARKETING union all select \'DE\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_DE_MARKETING UNION ALL select \'AU\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_AUS_MARKETING UNION ALL select \'IN\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_IN_MARKETING UNION ALL select \'UAE\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_UAE_MARKETING UNION ALL select \'IT\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_IT_MARKETING UNION ALL select \'ES\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_ESP_MARKETING union all select \'FR\' as Region ,DATE ,ASIN ,CAMPAIGN_TYPE ,PROFILEID ,CAMPAIGNID ,CAMPAIGNNAME ,ADGROUPID ,ADGROUPNAME ,KEYWORDTEXT ,CAMPAIGNSTATUS ,ADID ,TARGETINGEXPRESSION ,TARGETINGTEXT ,CURRENCY ,NEWTOBRANDORDERS ,NEWTOBRANDSALES ,NEWTOBRANDUNITS ,IMPRESSIONS ,CLICKS ,SPEND ,SALES ,CONVERSIONS ,CONVERSIONSSAMESKU ,SALESSAMESKU ,OTHERSKUSALES ,CONVERSIONSOTHERSKU ,SALES_USD ,ORDERS ,QUANTITY ,SESSIONS ,PAGEVIEWS ,ASIN_MAPPING ,ASIN_NEW ,WEIGHT ,BRAND ,MOTHER_SKU ,PRODUCTCATEGORY ,TYPEOFTEA ,TYPEOFPRODUCT ,PRODUCTNAME ,COMMONSKU_ID ,TYPEOFPACK from VAHDAM_DB.MAPLEMONK.AMAZONADS_FR_MARKETING",
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
            