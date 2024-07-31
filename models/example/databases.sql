{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_THREE60YOU AS select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,SUM(NVL(Cc.Conversions,0)) Consult_Booked ,\'Facebook\' Channel ,\'Facebook Three60you\' Facebook_Account from RPSG_DB.MAPLEMONK.FB_THREE60YOU_CUSTOMCAMPAIGNS_DATA FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FB_THREE60YOU_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:\"7d_click\") Conversion_Value from RPSG_DB.MAPLEMONK.FB_THREE60YOU_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id,ad_name,date_start ,SUM(C.value:value) Conversions from RPSG_DB.MAPLEMONK.FB_THREE60YOU_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_custom\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Cc ON Fb.ad_id = Cc.ad_id and Fb.date_start=Cc.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_THREE60YOU AS select ADSET_NAME, ADSET_ID,ad_id ::varchar ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ::varchar ACCOUNT_ID ,CAMPAIGN_NAME ,product.product ,CAMPAIGN_ID ::varchar CAMPAIGN_ID, DATE_START ::DATE \"DATE\" ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,upper(case when lower(campaign_name) like \'%consultation%\' then \'CONSULTATIONS\' when lower(CAMPAIGN_NAME) like \'three60plus%\' then \'Three60plus\' else \'Three60\' end) AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value ,sum(consult_booked) consult_booked ,\'three60\' as data_source ,NULL AS RR_FLAG from RPSG_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_THREE60YOU three60 left join ( select * from( select \"Campaign Name Contains\", product, channel as channel1 ,row_number() over(partition by lower(product),lower(channel) order by 1)rw from rpsg_db.maplemonk.three60_paid where lower(channel) = \'facebook\' ) where rw= 1 )product on lower(three60.CAMPAIGN_NAME) like \'%\' || lower(product.\"Campaign Name Contains\") || \'%\' group by ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT,product.product,data_source UNION ALL select \"ad_group.name\",\"ad_group_ad.ad.id\",NULL,NULL,NULL,NULL ,\"campaign.name\" ,product.product ,\"campaign.id\" ::varchar \"campaign.id\",\"segments.date\"::DATE DATE ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google\' Channel ,upper(case when lower(\"campaign.name\") like \'%consultation%\' then \'CONSULTATIONS\' when lower(\"campaign.name\") like \'three60+%\' then \'Three60Plus\' else \'Three60\' end) AS ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value ,sum(consult_booked) as consult_booked ,\'three60\' as data_source ,NULL AS RR_FLAG from rpsg_db.maplemonk.gads_three60you_ad_group_ad_report three60 left join ( select * from( select \"Campaign Name Contains\", product, channel as channel1 ,row_number() over(partition by lower(product),lower(channel) order by 1)rw from rpsg_db.maplemonk.three60_paid where lower(channel) = \'google\' ) where rw= 1 )product on lower(three60.\"campaign.name\") like \'%\' || lower(product.\"Campaign Name Contains\") || \'%\' left join ( select \"ad_group.id\"as ad_group_id, \"ad_group_ad.ad_group\" as ad_group, \"segments.ad_network_type\" as network_type, \"campaign.id\" as cid, \"segments.date\" as s_date, \"ad_group_ad.ad.id\" as ad_id, sum(\"metrics.conversions\") as consult_booked from rpsg_db.maplemonk.gads_three60younew_google_ads_ad_data where lower(\"segments.conversion_action_name\") like \'%consult_booked%\' group by 1,2,3,4,5,6 )cb on three60.\"segments.date\" = cb.s_date and lower(cb.ad_group_id) = lower(three60.\"ad_group.id\") and lower(cb.ad_group) = lower(three60.\"ad_group_ad.ad_group\") and lower(cb.cid) = lower(three60.\"campaign.id\") and lower(\"segments.ad_network_type\") = lower(cb.network_type) and lower(cb.ad_id) = lower(three60.\"ad_group_ad.ad.id\") group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\",product.product,data_source UNION all select NULL, NULL, NULL, NULL,NULL,NULL ,\"campaign.name\" ,product.product ,\"campaign.id\" ::varchar \"campaign.id\" , \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,upper(case when lower(\"campaign.name\") like \'%consultation%\' then \'CONSULTATIONS\' when lower(\"campaign.name\") like \'three60+%\' then \'Three60Plus\' else \'Three60\' end) AS ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value ,sum(cc.consult_booked) consult_booked ,\'three60\' as data_source ,NULL AS RR_FLAG from RPSG_DB.maplemonk.gads_three60you_campaign_data three60 left join ( select * from( select \"Campaign Name Contains\", product, channel as channel1 ,row_number() over(partition by lower(product),lower(channel) order by 1)rw from rpsg_db.maplemonk.three60_paid where lower(channel) = \'google\' ) where rw= 1 )product on lower(three60.\"campaign.name\") like \'%\' || lower(product.\"Campaign Name Contains\") || \'%\' left join ( select \"campaign.id\" as cid, \"segments.date\" as date, sum(\"metrics.conversions\") as consult_booked from rpsg_db.maplemonk.gads_three60you_campaign_action_data where lower(\"segments.conversion_action_name\") like \'%consult_booked%\' group by 1,2 )CC on lower(three60.\"campaign.id\") = lower(CC.cid) and cc.date = three60.\"segments.date\" where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\",product.product,data_source union all Select NULL AS ACCOUNT_NAME ,PROFILEID ,ADID ::varchar ad_id ,NULL AS AD_NAME ,upper(CAMPAIGNNAME) CAMPAIGN_NAME ,CAMPAIGNID ::varchar CAMPAIGN_ID ,upper(adGroupName) adGroupName ,PRODUCT_NAME as product ,adGroupId ::varchar adGroupId ,DATE ::DATE DATE ,CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,dayname(try_to_date(date)) DAY_OF_WEEK ,year(try_to_date(date)) YEAR ,month(try_to_date(date)) MONTH ,\'AMAZON\' AS CHANNEL ,upper(case when profileid = \'3938387603612472\' then \'three60\' else \'three60plus\' end) AS ACCOUNT ,sum(CLICKS)CLICKS ,sum(SPEND)SPEND ,sum(IMPRESSIONS)IMPRESSIONS ,sum(CONVERSIONS) CONVERSIONS ,sum(AdSales) AdSales ,null as consult_booked ,\'amazon\' as data_source ,NULL AS RR_FLAG FROM RPSG_DB.MAPLEMONK.RPSG_DB_AMAZONADS_MARKETING am left join (select * from (select marketplace_sku, sku, brand , category as Product_Category, \"Product Name\" AS PRODUCT_NAME, \"Pack Size\"AS Product_Pack, null as Report_Category, null as product_quantity, row_number() over (partition by lower(sku) order by 1) rw from rpsg_DB.maplemonk.three60_sku_master where sku is not null and lower(marketplace) like \'%amazon%\') where rw=1 ) S on lower(am.asin)=lower(s.marketplace_sku) where profileid in (\'3938387603612472\',\'3437629653399519\') group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 union all select null AS ADSET_NAME, null AS ADSET_ID, \"Ad Group ID\"::varchar ad_id, \"AdGroup Name\" AS ad_name, null as ACCOUNT_NAME, null as ACCOUNT_ID, \"Campaign Name\" as CAMPAIGN_NAME, null as PRODUCT, \"Campaign ID\"::varchar CAMPAIGN_ID, coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) AS DATE, null as AD_TYPE, null as AD_STRENGTH, null as AD_NETWORK_TYPE, null as AD_URL, dayname(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) Day_of_Week, YEAR(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) AS \"YEAR\", MONTH(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) AS \"MONTH\", \'Flipkart\' as CHANNEL, \'Three60\' as ACCOUNT, SUM(Clicks) Clicks, SUM(\"Ad Spend\") as Spend, SUM(VIEWS) AS Impressions, SUM(\"Units Sold (Direct)\") AS Conversions, SUM(\"Direct Revenue\") AS Conversion_Value, null as consult_booked, \'Flipkart\' as data_source ,NULL AS RR_FLAG from rpsg_db.maplemonk.three60you_flipkart_ads group by ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, ACCOUNT,data_source UNION ALL select null AS ADSET_NAME, null AS ADSET_ID, \"Ad Group ID\"::varchar ad_id, \"AdGroup Name\" AS ad_name, null as ACCOUNT_NAME, null as ACCOUNT_ID, \"Campaign Name\" as CAMPAIGN_NAME, null as PRODUCT, \"Campaign ID\"::varchar CAMPAIGN_ID, coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) AS DATE, null as AD_TYPE, null as AD_STRENGTH, null as AD_NETWORK_TYPE, null as AD_URL, dayname(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) Day_of_Week, YEAR(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) AS \"YEAR\", MONTH(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) AS \"MONTH\", \'Flipkart\' as CHANNEL, \'Three60plus\' as ACCOUNT, SUM(Clicks) Clicks, SUM(\"Ad Spend\") as Spend, SUM(VIEWS) AS Impressions, SUM(\"Units Sold (Direct)\") AS Conversions, SUM(\"Direct Revenue\") AS Conversion_Value, null as consult_booked, \'Flipkart\' as data_source ,NULL AS RR_FLAG from rpsg_db.maplemonk.three60you_flipkart_ads_three60plus group by ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,\"Campaign Name\", \"Campaign ID\", DATE, CHANNEL, ACCOUNT,data_source UNION ALL ( WITH RR_DATA AS ( select sc.* ,RR_Flag_Spends, RR_SOURCE from rpsg_DB.maplemonk.SALES_CONSOLIDATED_THREE60 sc left join ( (select distinct ifnull(reference_orders,\'\') RR_REF ,1 as RR_Flag_Spends, \'AMAZON\' AS RR_SOURCE from rpsg_db.maplemonk.three60you_amazon_rr_orders) union all (select distinct ifnull(reference_orders,\'\') ,1 as RR_Flag_Spends, \'FLIPKART\' AS RR_SOURCE from rpsg_db.maplemonk.three60you_flipkart_rr_orders) )rr on sc.reference_code = rr.rr_ref where order_date::date >= \'2024-03-20\' and RR_Flag_Spends = 1 AND FINAL_STATUS NOT IN (\'CANCELLED\', \'RTO\') ), T001 AS( SELECT RR_SOURCE, ORDER_DATE AS DATE, SKU, PRODUCT_NAME_MAPPED, CATEGORY, SKU_TYPE, SUBORDER_QUANTITY, SELLING_PRICE AS REVENUE, CASE WHEN RR_SOURCE = \'AMAZON\' THEN \'185\' ELSE \'45\' END AS COMM_1 FROM RR_DATA ), T002 AS ( SELECT RR_SOURCE, DATE, SKU, PRODUCT_NAME_MAPPED, CATEGORY, SKU_TYPE, SUM(REVENUE), SUM(COMM_1 * SUBORDER_QUANTITY) AS COMMISION, SUM(REVENUE + (COMM_1 * SUBORDER_QUANTITY)) AS RR_SPENDS FROM T001 GROUP BY 1,2,3,4,5,6 ) SELECT NULL AS ADSET_NAME , NULL AS ADSET_ID , NULL AS ad_id , NULL AS ad_name , NULL AS ACCOUNT_NAME , NULL AS ACCOUNT_ID , NULL AS CAMPAIGN_NAME , T.sku AS product , NULL AS CAMPAIGN_ID , T.DATE AS DATE , NULL AS AD_TYPE , NULL AS AD_STRENGTH , NULL AS AD_NETWORK_TYPE , NULL AS AD_URL , dayname(DATE) Day_of_Week , YEAR(DATE) AS \"YEAR\" , MONTH(DATE) AS \"MONTH\" , \'AMAZON\' AS CHANNEL , RR_SOURCE AS ACCOUNT , NULL AS Clicks , T.RR_SPENDS AS Spend , NULL AS Impressions , NULL AS Conversions , NULL AS Conversion_Value , NULL AS consult_booked , \'AMAZON\' AS data_source , 1 AS RR_FLAG FROM T002 T LEFT JOIN ( SELECT marketplace_sku, sku, brand, category AS Product_Category, \"Product Name\" AS PRODUCT_NAME, \"Pack Size\" AS Product_Pack, NULL AS Report_Category, NULL AS product_quantity, ROW_NUMBER() OVER (PARTITION BY LOWER(sku) ORDER BY 1) AS rw FROM rpsg_DB.maplemonk.three60_sku_master WHERE sku IS NOT NULL AND (LOWER(marketplace) LIKE \'%amazon%\' OR LOWER(marketplace) LIKE \'%flipkart%\') ) S ON LOWER(T.SKU) = LOWER(S.SKU) AND S.rw = 1)",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            