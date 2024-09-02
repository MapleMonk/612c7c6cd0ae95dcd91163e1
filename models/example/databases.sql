{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.zouk_pandl as select coalesce(ORDER_DATE,marketing_date) as Date ,sc.MARKETPLACE ,FINAL_MARKETPLACE ,coalesce(CHANNEL,msc.marketing_channel) channel ,SOURCE ,REFERENCE_CODE ,SKU ,sc.COMMONSKU ,PRODUCT_CATEGORY ,PRODUCT_SUB_CATEGORY ,sc.PRODUCT_TYPE ,sc.COLLECTION ,sc.PRINT ,sc.category_code ,(ifnull(sc.BAU_ONLINE,0)) BAU_MRP_SALES ,(ifnull(SELLING_PRICE,0)) SALES ,(case when tm.marketplace is not null then (case when(ifnull(sc.BAU_ONLINE,0))>= (ifnull(SELLING_PRICE,0)) then (ifnull(sc.BAU_ONLINE,0)) - (ifnull(SELLING_PRICE,0)) else 0 end) else 0 end) + (ifnull(mc.commission_value,0) * ifnull(selling_price,0)) as TRADE_MARGIN ,case when tm.marketplace is null then ( (case when(ifnull(sc.BAU_ONLINE,0))>= (ifnull(SELLING_PRICE,0)) then (ifnull(sc.BAU_ONLINE,0)) - (ifnull(SELLING_PRICE,0)) else 0 end)) else 0 end BAU_DISCOUNT ,coalesce(fsm.tax_rate,0.12) * (ifnull(SELLING_PRICE,0) -ifnull(tax,0)) as GST ,(cm.commission_value * ifnull(selling_price,0)) CHANNEL_MARGIN ,(RS.commission_value * ifnull(selling_price,0)) RETURNS ,safe_divide(msc.spend,count(*) over(partition by order_date,channel)) as spend ,cogs.cogs as cogs ,coalesce((store.commission_value * ifnull(selling_price,0)), safe_divide(store.Actual_Value,count(*) over(partition by last_day(coalesce(ORDER_DATE,marketing_date)),lower(sc.source)))) offline_store_cost from (select * from maplemonk.zouk_sales_consolidated where not(lower(ifnull(ORDER_STATUS,\'\')) like \'%cancel%\' or lower(ifnull(FINAL_SHIPPING_STATUS,\'\')) like \'%cancel%\') ) SC left join ( select distinct MARKETPLACE from `MapleMonk.zouk_db_Marketplace_Commissions` where upper(PANDL_CATEGORY) = \'TRADE MARGIN\' ) tm on lower(sc.marketplace) = lower(tm.MARKETPLACE) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,MARKETPLACE ,cast(replace(ifnull(commission_value,\'0\'),\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Value as float64) as Actual_Value from `MapleMonk.zouk_db_Marketplace_Commissions` where upper(PANDL_CATEGORY) = \'TRADE MARGIN\' and lower(marketplace) like \'%myntra%\' )mc on cast(ORDER_DATE as date) >= cast(mc.START_DATE as date) and cast(ORDER_DATE as date) <= cast(mc.END_DATE as date) and lower(sc.marketplace) = lower(mc.MARKETPLACE) left join (Select * from ( select *,row_number() over(partition by commonsku order by 1)rw from maplemonk.final_sku_master )where rw = 1 ) fsm on fsm.commonsku = sc.commonsku left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,MARKETPLACE ,PANDL_CATEGORY ,cast(replace(commission_value,\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Value as float64) as Actual_Value from `MapleMonk.zouk_db_Marketplace_Commissions` where upper(PANDL_CATEGORY) = \'CHANNEL MARGIN\' )cm on cast(ORDER_DATE as date) >= cast(cm.START_DATE as date) and cast(ORDER_DATE as date) <= cast(cm.END_DATE as date) and lower(sc.marketplace) = lower(cm.MARKETPLACE) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,MARKETPLACE ,cast(replace(Estimated_Return_Share,\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Return_Value as float64) as Actual_Value from MapleMonk.zouk_db_returns_share )rs on cast(ORDER_DATE as date) >= cast(rs.START_DATE as date) and cast(ORDER_DATE as date) <= cast(rs.END_DATE as date) and lower(sc.marketplace) = lower(rs.MARKETPLACE) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,category_code ,cast(cogs as float64) as cogs from MapleMonk.zouk_db_sku_mrp_cogs )cogs on cast(ORDER_DATE as date) >= cast(cogs.START_DATE as date) and cast(ORDER_DATE as date) <= cast(cogs.END_DATE as date) and lower(sc.category_code) = lower(cogs.category_code) Full Outer join ( select date as marketing_date ,channel as marketing_channel ,sum(ifnull(spend,0))spend from maplemonk.zouk_MARKETING_CONSOLIDATED group by 1,2 )msc on cast(msc.marketing_date as date) = cast(sc.order_Date as date) and lower(msc.marketing_channel) = lower(sc.channel) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,cast(replace(commission_value,\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Value as float64) as Actual_Value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` )store on cast(ORDER_DATE as date) >= cast(store.START_DATE as date) and cast(ORDER_DATE as date) <= cast(store.END_DATE as date) and lower(sc.source) = lower(store.store) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            