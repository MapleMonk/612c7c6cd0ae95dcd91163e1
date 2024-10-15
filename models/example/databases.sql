{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.zouk_pandl as with cte as ( select coalesce(ORDER_DATE,marketing_nocategory_date,marketing_date,store_rent.START_DATE,store_cam.START_DATE ,store_elec.start_date,store_int.START_DATE,store_hvac.START_DATE,store_other.start_date,store_salary.start_date,store_incentive.start_date,store_bank.start_date) as Date ,sc.MARKETPLACE ,coalesce( FINAL_MARKETPLACE, case when lower(coalesce(msc.marketing_channel,mscc.marketing_nocategory_channel)) like \'%amazon%\' then \'AMAZON\' when lower(coalesce(msc.marketing_channel,mscc.marketing_nocategory_channel)) like any (\'%google%\',\'%facebook%\') then \'website\' else lower(coalesce(msc.marketing_channel,mscc.marketing_nocategory_channel)) end , case when coalesce(store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store, store_other.store,store_salary.sTORE,store_incentive.store,store_bank.store) is not null then \'EBO\' end) as FINAL_MARKETPLACE ,coalesce(CHANNEL,msc.marketing_channel,mscc.marketing_nocategory_channel) channel ,coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store) SOURCE ,REFERENCE_CODE ,SKU ,sc.COMMONSKU ,coalesce(PRODUCT_CATEGORY,msc.category) as PRODUCT_CATEGORY ,PRODUCT_SUB_CATEGORY ,sc.PRODUCT_TYPE ,sc.COLLECTION ,sc.PRINT ,sc.category_code ,(ifnull(sc.BAU_ONLINE,0)) BAU_MRP_SALES ,(ifnull(SELLING_PRICE,0)) SALES ,(case when tm.marketplace is not null then (case when(ifnull(sc.BAU_ONLINE,0))>= (ifnull(SELLING_PRICE,0)) then (ifnull(sc.BAU_ONLINE,0)) - (ifnull(SELLING_PRICE,0)) else 0 end) else 0 end) + (ifnull(mc.commission_value,0) * ifnull(selling_price,0)) as TRADE_MARGIN ,case when tm.marketplace is null then ( (case when(ifnull(sc.BAU_ONLINE,0))>= (ifnull(SELLING_PRICE,0)) then (ifnull(sc.BAU_ONLINE,0)) - (ifnull(SELLING_PRICE,0)) else 0 end)) else 0 end BAU_DISCOUNT ,(coalesce(fsm.tax_rate,0.12) * (ifnull(SELLING_PRICE,0) - ifnull((RS.commission_value * ifnull(selling_price,0)),0) ))/(1+coalesce(fsm.tax_rate,0.12)) as GST ,(cm.commission_value * ifnull(selling_price,0)) CHANNEL_MARGIN ,(RS.commission_value * ifnull(selling_price,0)) RETURNS ,(lc.commission_value * ifnull(selling_price,0)) LOGISTICS_COST ,safe_divide(msc.spend,count(*) over(partition by (coalesce(order_date,marketing_nocategory_date,marketing_date)),lower(coalesce(channel,marketing_channel,marketing_nocategory_channel)) ,lower(coalesce(PRODUCT_CATEGORY,msc.category)))) as spend2 ,safe_divide(mscc.spend,count(*) over(partition by (coalesce(order_date,marketing_nocategory_date,marketing_date)),lower(coalesce(channel,marketing_channel,marketing_nocategory_channel)) )) as spend1 ,safe_divide(msc.brand_spend,count(*) over(partition by (coalesce(order_date,marketing_nocategory_date,marketing_date)),lower(coalesce(channel,marketing_channel,marketing_nocategory_channel)) ,lower(coalesce(PRODUCT_CATEGORY,msc.category)))) as brand_spend2 ,safe_divide(mscc.brand_spend,count(*) over(partition by (coalesce(order_date,marketing_nocategory_date,marketing_date)),lower(coalesce(channel,marketing_channel,marketing_nocategory_channel)) )) as brand_spend1 ,cogs.cogs as cogs ,safe_divide(store_rent.commission_value, count(*) over(partition by last_day(COALESCE(ORDER_DATE,marketing_nocategory_date, marketing_date, store_rent.START_DATE, store_cam.START_DATE, store_elec.start_date, store_int.START_DATE, store_hvac.START_DATE, store_other.start_date, store_salary.start_date, store_incentive.start_date, store_bank.start_date)) ,lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store)))) store_rent ,safe_divide(store_cam.commission_value, count(*) over(partition by last_day(COALESCE(ORDER_DATE,marketing_nocategory_date, marketing_date, store_rent.START_DATE, store_cam.START_DATE, store_elec.start_date, store_int.START_DATE, store_hvac.START_DATE, store_other.start_date, store_salary.start_date, store_incentive.start_date, store_bank.start_date)) ,lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store)))) store_cam ,safe_divide(store_elec.commission_value, count(*) over(partition by last_day(COALESCE(ORDER_DATE,marketing_nocategory_date, marketing_date, store_rent.START_DATE, store_cam.START_DATE, store_elec.start_date, store_int.START_DATE, store_hvac.START_DATE, store_other.start_date, store_salary.start_date, store_incentive.start_date, store_bank.start_date)) ,lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store)))) store_elec ,safe_divide(store_int.commission_value, count(*) over(partition by last_day(COALESCE(ORDER_DATE, marketing_date,marketing_nocategory_date, store_rent.START_DATE, store_cam.START_DATE, store_elec.start_date, store_int.START_DATE, store_hvac.START_DATE, store_other.start_date, store_salary.start_date, store_incentive.start_date, store_bank.start_date)) ,lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store)))) store_int ,safe_divide(store_hvac.commission_value, count(*) over(partition by last_day(COALESCE(ORDER_DATE, marketing_date,marketing_nocategory_date, store_rent.START_DATE, store_cam.START_DATE, store_elec.start_date, store_int.START_DATE, store_hvac.START_DATE, store_other.start_date, store_salary.start_date, store_incentive.start_date, store_bank.start_date)) ,lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store)))) store_hvac ,safe_divide(store_other.commission_value, count(*) over(partition by last_day(COALESCE(ORDER_DATE, marketing_date,marketing_nocategory_date, store_rent.START_DATE, store_cam.START_DATE, store_elec.start_date, store_int.START_DATE, store_hvac.START_DATE, store_other.start_date, store_salary.start_date, store_incentive.start_date, store_bank.start_date)) ,lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store)))) store_other ,safe_divide(store_salary.commission_value, count(*) over(partition by last_day(COALESCE(ORDER_DATE, marketing_date,marketing_nocategory_date, store_rent.START_DATE, store_cam.START_DATE, store_elec.start_date, store_int.START_DATE, store_hvac.START_DATE, store_other.start_date, store_salary.start_date, store_incentive.start_date, store_bank.start_date)) ,lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store ,store_salary.sTORE,store_incentive.store,store_bank.store)))) store_salary ,(ifnull(store_incentive.commission_value,0) * ifnull(selling_price,0)) store_incentive ,(ifnull(store_bank.commission_value,0) * ifnull(selling_price,0)) store_bank from (select * from maplemonk.zouk_sales_consolidated where not(lower(ifnull(ORDER_STATUS,\'\')) like \'%cancel%\' or lower(ifnull(FINAL_SHIPPING_STATUS,\'\')) like \'%cancel%\') ) SC left join ( select distinct MARKETPLACE from `MapleMonk.zouk_db_Marketplace_Commissions` where upper(PANDL_CATEGORY) = \'TRADE MARGIN\' ) tm on lower(sc.marketplace) = lower(tm.MARKETPLACE) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,MARKETPLACE ,cast(replace(ifnull(commission_value,\'0\'),\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Value as float64) as Actual_Value from `MapleMonk.zouk_db_Marketplace_Commissions` where upper(PANDL_CATEGORY) = \'TRADE MARGIN\' and lower(marketplace) like \'%myntra%\' )mc on cast(ORDER_DATE as date) >= cast(mc.START_DATE as date) and cast(ORDER_DATE as date) <= cast(mc.END_DATE as date) and lower(sc.marketplace) = lower(mc.MARKETPLACE) left join (Select * from ( select *,row_number() over(partition by commonsku order by 1)rw from maplemonk.final_sku_master )where rw = 1 ) fsm on lower(fsm.commonsku) = lower(sc.commonsku) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,MARKETPLACE ,PANDL_CATEGORY ,cast(replace(commission_value,\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Value as float64) as Actual_Value from `MapleMonk.zouk_db_Marketplace_Commissions` where upper(PANDL_CATEGORY) = \'CHANNEL MARGIN\' )cm on cast(ORDER_DATE as date) >= cast(cm.START_DATE as date) and cast(ORDER_DATE as date) <= cast(cm.END_DATE as date) and lower(sc.marketplace) = lower(cm.MARKETPLACE) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,MARKETPLACE ,cast(replace(Estimated_Return_Share,\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Return_Value as float64) as Actual_Value from MapleMonk.zouk_db_returns_share )rs on cast(ORDER_DATE as date) >= cast(rs.START_DATE as date) and cast(ORDER_DATE as date) <= cast(rs.END_DATE as date) and lower(sc.marketplace) = lower(rs.MARKETPLACE) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,category_code ,cast(replace(cogs,\',\',\'\') as float64) as cogs from MapleMonk.zouk_db_sku_mrp_cogs )cogs on cast(ORDER_DATE as date) >= cast(cogs.START_DATE as date) and cast(ORDER_DATE as date) <= cast(cogs.END_DATE as date) and lower(sc.category_code) = lower(cogs.category_code) Full Outer join ( select date as marketing_date ,channel as marketing_channel ,category ,sum(ifnull(spend,0))spend ,sum(case when lower(replace(campaign_name,\'_\',\'$\')) like \'%$top$%\' then ifnull(spend,0) else 0 end) as Brand_Spend from maplemonk.zouk_MARKETING_CONSOLIDATED where category is not null group by 1,2,3 )msc on cast(msc.marketing_date as date) = cast(sc.order_Date as date) and lower(msc.marketing_channel) = lower(sc.channel) and lower(sc.PRODUCT_CATEGORY) = lower(msc.category) Full Outer join ( select date as marketing_nocategory_date ,channel as marketing_nocategory_channel ,sum(ifnull(spend,0))spend ,sum(case when lower(replace(campaign_name,\'_\',\'$\')) like \'%$top$%\' then ifnull(spend,0) else 0 end) as Brand_Spend from maplemonk.zouk_MARKETING_CONSOLIDATED where category is null group by 1,2 )mscc on cast(mscc.marketing_nocategory_date as date) = coalesce(cast(sc.order_Date as date),cast(msc.marketing_date as date)) and lower(mscc.marketing_nocategory_channel) = coalesce(lower(sc.channel),lower(msc.marketing_channel)) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,case when current_date() > PARSE_DATE(\'%d-%b-%y\',END_DATE) then cast(replace(Actual_Value,\'%\',\'\') as float64) else (cast(replace(Actual_Value,\'%\',\'\') as float64)/cast(EXTRACT(DAY FROM PARSE_DATE(\'%d-%b-%y\',END_DATE)) as int64))* cast(EXTRACT (DAY FROM current_date()) as int64) end as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) = \'rent\' )store_rent on cast(COALESCE(ORDER_DATE,marketing_nocategory_date, marketing_date) as date) >= cast(store_rent.START_DATE as date) and cast(COALESCE(ORDER_DATE,marketing_nocategory_date, marketing_date) as date) <= cast(store_rent.END_DATE as date) and lower(sc.source) = lower(store_rent.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,case when current_date() > PARSE_DATE(\'%d-%b-%y\',END_DATE) then cast(replace(Actual_Value,\'%\',\'\') as float64) else (cast(replace(Actual_Value,\'%\',\'\') as float64)/cast(EXTRACT(DAY FROM PARSE_DATE(\'%d-%b-%y\',END_DATE)) as int64))* cast(EXTRACT (DAY FROM current_date()) as int64) end as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'%cam charges%\' )store_cam on cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.START_DATE) as date) >= cast(store_cam.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.end_date) as date) <= cast(store_cam.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store)) = lower(store_cam.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,case when current_date() > PARSE_DATE(\'%d-%b-%y\',END_DATE) then cast(replace(Actual_Value,\'%\',\'\') as float64) else (cast(replace(Actual_Value,\'%\',\'\') as float64)/cast(EXTRACT(DAY FROM PARSE_DATE(\'%d-%b-%y\',END_DATE)) as int64))* cast(EXTRACT (DAY FROM current_date()) as int64) end as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'%electricity exp%\' )store_elec on cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.START_DATE,store_cam.START_DATE) as date) >= cast(store_elec.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.end_date,store_cam.END_DATE) as date) <= cast(store_elec.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store,store_cam.store)) = lower(store_elec.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,case when current_date() > PARSE_DATE(\'%d-%b-%y\',END_DATE) then cast(replace(Actual_Value,\'%\',\'\') as float64) else (cast(replace(Actual_Value,\'%\',\'\') as float64)/cast(EXTRACT(DAY FROM PARSE_DATE(\'%d-%b-%y\',END_DATE)) as int64))* cast(EXTRACT (DAY FROM current_date()) as int64) end as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'%internet exp%\' )store_int on cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.START_DATE,store_cam.START_DATE,store_elec.START_DATE) as date) >= cast(store_int.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.end_date,store_cam.END_DATE,store_elec.END_DATE) as date) <= cast(store_int.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store)) = lower(store_int.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,case when current_date() > PARSE_DATE(\'%d-%b-%y\',END_DATE) then cast(replace(Actual_Value,\'%\',\'\') as float64) else (cast(replace(Actual_Value,\'%\',\'\') as float64)/cast(EXTRACT(DAY FROM PARSE_DATE(\'%d-%b-%y\',END_DATE)) as int64))* cast(EXTRACT (DAY FROM current_date()) as int64) end as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'hvac%\' )store_hvac on cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.START_DATE,store_cam.START_DATE,store_elec.START_DATE,store_int.START_DATE) as date) >= cast(store_hvac.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.end_date,store_cam.END_DATE,store_elec.END_DATE,store_int.END_DATE) as date) <= cast(store_hvac.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store)) = lower(store_hvac.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,case when current_date() > PARSE_DATE(\'%d-%b-%y\',END_DATE) then cast(replace(Actual_Value,\'%\',\'\') as float64) else (cast(replace(Actual_Value,\'%\',\'\') as float64)/cast(EXTRACT(DAY FROM PARSE_DATE(\'%d-%b-%y\',END_DATE)) as int64))* cast(EXTRACT (DAY FROM current_date()) as int64) end as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'other store expenses%\' )store_other on cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.START_DATE,store_cam.START_DATE,store_elec.START_DATE,store_int.START_DATE,store_hvac.START_DATE) as date) >= cast(store_other.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.end_date,store_cam.END_DATE,store_elec.END_DATE,store_int.END_DATE,store_hvac.END_DATE) as date) <= cast(store_other.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store)) = lower(store_other.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,case when current_date() > PARSE_DATE(\'%d-%b-%y\',END_DATE) then cast(replace(Actual_Value,\'%\',\'\') as float64) else (cast(replace(Actual_Value,\'%\',\'\') as float64)/cast(EXTRACT(DAY FROM PARSE_DATE(\'%d-%b-%y\',END_DATE)) as int64))* cast(EXTRACT (DAY FROM current_date()) as int64) end as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'staff salary%\' )store_salary on cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.START_DATE,store_cam.START_DATE,store_elec.START_DATE,store_int.START_DATE,store_hvac.START_DATE,store_other.START_DATE) as date) >= cast(store_salary.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.end_date,store_cam.END_DATE,store_elec.END_DATE,store_int.END_DATE,store_hvac.END_DATE,store_other.END_DATE) as date) <= cast(store_salary.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store)) = lower(store_salary.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,cast(replace(Actual_Value,\'%\',\'\') as float64)/100 as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'staff incentive%\' )store_incentive on cast(coalesce(ORDER_DATE,marketing_date,store_rent.START_DATE,marketing_nocategory_date,store_cam.START_DATE,store_elec.START_DATE,store_int.START_DATE,store_hvac.START_DATE,store_other.START_DATE,store_salary.START_DATE) as date) >= cast(store_incentive.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_nocategory_date,marketing_date,store_rent.end_date,store_cam.END_DATE,store_elec.END_DATE,store_int.END_DATE,store_hvac.END_DATE,store_other.END_DATE,store_salary.END_DATE) as date) <= cast(store_incentive.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store,store_salary.store)) = lower(store_incentive.store) full outer join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,store ,cast(replace(Actual_Value,\'%\',\'\') as float64)/100 as commission_value from `MapleMonk.zouk_db_OFFLINE_STORE_COSTS` where lower(type) like \'bank comm%\' )store_bank on cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.START_DATE,store_cam.START_DATE,store_elec.START_DATE,store_int.START_DATE,store_hvac.START_DATE,store_other.START_DATE,store_salary.START_DATE,store_incentive.START_DATE) as date) >= cast(store_bank.START_DATE as date) and cast(coalesce(ORDER_DATE,marketing_date,marketing_nocategory_date,store_rent.end_date,store_cam.END_DATE,store_elec.END_DATE,store_int.END_DATE,store_hvac.END_DATE,store_other.END_DATE,store_salary.END_DATE,store_incentive.END_DATE) as date) <= cast(store_bank.END_DATE as date) and lower(coalesce(SOURCE,store_rent.store,store_cam.store,store_elec.store,store_int.store,store_hvac.store,store_other.store,store_salary.store,store_incentive.store)) = lower(store_bank.store) left join ( select PARSE_DATE(\'%d-%b-%y\',START_DATE) START_DATE ,PARSE_DATE(\'%d-%b-%y\',END_DATE) END_DATE ,MARKETPLACE ,cast(replace(Estimated_Logistics_Cost,\'%\',\'\') as float64)/100 as commission_value ,cast(Actual_Value as float64) as Actual_Value from `MapleMonk.zouk_db_LOGISTICS_COSTS` )lc on cast(ORDER_DATE as date) >= cast(lc.START_DATE as date) and cast(ORDER_DATE as date) <= cast(lc.END_DATE as date) and lower(sc.marketplace) = lower(lc.MARKETPLACE) ) select * , ifnull(store_rent,0) + ifnull(store_cam,0) + ifnull(store_elec,0) + ifnull(store_int,0)+ ifnull(store_hvac,0) + ifnull(store_other,0) + ifnull(store_salary,0) + ifnull(store_incentive,0) + ifnull(store_bank,0) as offline_store_cost ,ifnull(spend1,0) + ifnull(spend2,0) as spend ,ifnull(brand_spend1,0) + ifnull(brand_spend2,0) as brand_spend from cte ;",
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
            