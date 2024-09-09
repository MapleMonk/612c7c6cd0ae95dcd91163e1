{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table bsc_db.maplemonk.daily_Revenue_tracker as select \'Sales\' as metric, a.date, a.marketing_channel, a.marketing_channel_aggregated, a.channel_type, a.brand, a.total_Sales_excl_cancl value, case when marketing_channel_aggregated = \'CRM\' then div0(b.target, right(last_day(a.date),2)) else div0(b.target, right(last_day(a.date),2))/3 end daily_target from (SELECT date::Date AS date, case when upper(marketing_channel) in (\'UNPAID\',\'PAID GOOGLE\',\'PAID FACEBOOK\') then \'Perf+Website\' when upper(marketing_channel) in (\'CRM\',\'WA\', \'EMAIL\',\'ABANDONED CART\',\'TELLEPHANT\') then \'CRM\' else upper(marketing_channel) end AS marketing_Channel_Aggregated, case when upper(marketing_channel) in (\'CRM\',\'WA\', \'EMAIL\',\'ABANDONED CART\',\'TELLEPHANT\') then \'CRM\' else upper(marketing_channel) end as marketing_channel, upper(channel_type) channel_type, upper(brand) brand, sum(ifnull(total_sales_excl_cancl, 0)) as total_Sales_excl_cancl FROM bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND marketing_channel IN (\'UNPAID\',\'PAID GOOGLE\',\'PAID FACEBOOK\',\'CRM\',\'ABANDONED CART\',\'EMAIL\',\'WA\', \'TELLEPHANT\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' GROUP BY 1,2,3,4,5 ) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bsc\') b on b.CHANNEL_AGGREGATED = a.marketing_Channel_Aggregated and b.metric = \'Sales\' and date_trunc(\'month\',a.date) = b.month union all select \'Spend\' as metric, a.date, a.marketing_channel, a.marketing_channel_aggregated, a.channel_type, a.brand, a.total_marketing_spend value, div0(b.target, right(last_day(a.date),2))/2 daily_target from (SELECT date::Date AS date, case when upper(marketing_channel) in (\'PAID GOOGLE\',\'PAID FACEBOOK\') then \'Perf+Website\' end AS marketing_Channel_Aggregated, upper(channel_type) channel_type, upper(brand) brand, upper(marketing_channel) marketing_channel, sum(ifnull(marketing_spend_performance, 0)) as total_marketing_spend FROM bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND marketing_channel IN (\'PAID GOOGLE\',\'PAID FACEBOOK\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) and marketing_Channel_Aggregated is not null AND brand = \'Bombay Shaving Company\' GROUP BY 1,2,3,4,5 ) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bsc\') b on b.metric = \'Spend\' and date_trunc(\'month\',a.date) = b.month union all select \'Sales\' as metric, a.date, a.marketing_channel, a.marketing_channel_aggregated, a.channel_type, a.brand, a.total_Sales_excl_cancl value, case when marketing_channel_aggregated = \'CRM\' then div0(b.target, right(last_day(a.date),2)) else div0(b.target, right(last_day(a.date),2))/3 end daily_target from (SELECT date::Date AS date, case when upper(marketing_channel) in (\'UNPAID\',\'PAID GOOGLE\',\'PAID FACEBOOK\') then \'Perf+Website\' when upper(marketing_channel) in (\'CRM\',\'WA\', \'EMAIL\',\'ABANDONED CART\',\'TELLEPHANT\') then \'CRM\' else upper(marketing_channel) end AS marketing_Channel_Aggregated, case when upper(marketing_channel) in (\'CRM\',\'WA\', \'EMAIL\',\'ABANDONED CART\',\'TELLEPHANT\') then \'CRM\' else upper(marketing_channel) end as marketing_channel, upper(channel_type) channel_type, upper(brand) brand, sum(ifnull(total_sales_excl_cancl, 0)) as total_Sales_excl_cancl FROM bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND marketing_channel IN (\'UNPAID\',\'PAID GOOGLE\',\'PAID FACEBOOK\',\'CRM\',\'ABANDONED CART\',\'EMAIL\',\'WA\',\'TELLEPHANT\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombae\' GROUP BY 1,2,3,4,5 ) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bombae\') b on b.CHANNEL_AGGREGATED = a.marketing_Channel_Aggregated and b.metric = \'Sales\' and date_trunc(\'month\',a.date) = b.month union all select \'Spend\' as metric, a.date, a.marketing_channel, a.marketing_channel_aggregated, a.channel_type, a.brand, a.total_marketing_spend value, div0(b.target, right(last_day(a.date),2))/2 daily_target from (SELECT date::Date AS date, case when upper(marketing_channel) in (\'PAID GOOGLE\',\'PAID FACEBOOK\') then \'Perf+Website\' end AS marketing_Channel_Aggregated, upper(channel_type) channel_type, upper(brand) brand, upper(marketing_channel) marketing_channel, sum(ifnull(marketing_spend_performance, 0)) as total_marketing_spend FROM bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND marketing_channel IN (\'PAID GOOGLE\',\'PAID FACEBOOK\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) and marketing_Channel_Aggregated is not null AND brand = \'Bombae\' GROUP BY 1,2,3,4,5 ) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bombae\') b on b.metric = \'Spend\' and date_trunc(\'month\',a.date) = b.month ; create or replace table bsc_db.maplemonk.growth_equation as select \'Sessions\' as metric, a.date, a.channel_type, a.brand, a.Sessions value, div0(b.target, right(last_day(a.date),2)) daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, sum(sessions) sessions from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bsc\') b on b.metric = \'Sessions\' and date_trunc(\'month\',a.date) = b.month union all select \'Sales\' as metric, a.date, a.channel_type, a.brand, a.total_sales_excl_cancl value, div0(b.target, right(last_day(a.date),2)) daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, sum(ifnull(total_sales_excl_cancl, 0)) as total_Sales_excl_cancl from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select month, brand, channel_type, metric, sum(replace(target,\',\',\'\')) target from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bsc\' group by 1,2,3,4 ) b on b.metric = \'Sales\' and date_trunc(\'month\',a.date) = b.month union all select \'AOV\' as metric, a.date, a.channel_type, a.brand, a.aov value, b.target daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, div0(sum(ifnull(total_sales_excl_cancl,0)),sum(ifnull(orders_excl_cancl,0))) aov from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bsc\') b on b.metric = \'AOV\' and date_trunc(\'month\',a.date) = b.month union all select \'CR_percent\' as metric, a.date, a.channel_type, a.brand, a.CR_percent value, replace(b.target,\'%\',\'\') daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, div0(sum(total_orders),sum(sessions)) CR_percent from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bsc\') b on b.metric = \'Conversion%\' and date_trunc(\'month\',a.date) = b.month union all select \'Sessions\' as metric, a.date, a.channel_type, a.brand, a.Sessions value, div0(b.target, right(last_day(a.date),2)) daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, sum(sessions) sessions from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'NON CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'non-core\' and lower(brand) = \'bsc\') b on b.metric = \'Sessions\' and date_trunc(\'month\',a.date) = b.month union all select \'Sales\' as metric, a.date, a.channel_type, a.brand, a.total_sales_excl_cancl value, div0(b.target, right(last_day(a.date),2)) daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, sum(ifnull(total_sales_excl_cancl, 0)) as total_Sales_excl_cancl from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'NON CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select month, brand, channel_type, metric, sum(replace(target,\',\',\'\')) target from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'non-core\' and lower(brand) = \'bsc\' group by 1,2,3,4 ) b on b.metric = \'Sales\' and date_trunc(\'month\',a.date) = b.month union all select \'AOV\' as metric, a.date, a.channel_type, a.brand, a.aov value, b.target daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, div0(sum(ifnull(total_sales_excl_cancl,0)),sum(ifnull(orders_excl_cancl,0))) aov from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'NON CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'non-core\' and lower(brand) = \'bsc\') b on b.metric = \'AOV\' and date_trunc(\'month\',a.date) = b.month union all select \'CR_percent\' as metric, a.date, a.channel_type, a.brand, a.CR_percent value, replace(b.target,\'%\',\'\')daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, div0(sum(total_orders),sum(sessions)) CR_percent from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'NON CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombay Shaving Company\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'non-core\' and lower(brand) = \'bsc\') b on b.metric = \'Conversion%\' and date_trunc(\'month\',a.date) = b.month union all select \'Sessions\' as metric, a.date, a.channel_type, a.brand, a.Sessions value, div0(b.target, right(last_day(a.date),2)) daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, sum(sessions) sessions from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombae\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bombae\') b on b.metric = \'Sessions\' and date_trunc(\'month\',a.date) = b.month union all select \'Sales\' as metric, a.date, a.channel_type, a.brand, a.total_sales_excl_cancl value, div0(b.target, right(last_day(a.date),2)) daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, sum(ifnull(total_sales_excl_cancl, 0)) as total_Sales_excl_cancl from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombae\' group by 1,2,3) a left join (select month, brand, channel_type, metric, sum(replace(target,\',\',\'\')) target from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bombae\' group by 1,2,3,4 ) b on b.metric = \'Sales\' and date_trunc(\'month\',a.date) = b.month union all select \'AOV\' as metric, a.date, a.channel_type, a.brand, a.aov value, b.target daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, div0(sum(ifnull(total_sales_excl_cancl,0)),sum(ifnull(orders_excl_cancl,0))) aov from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombae\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bombae\') b on b.metric = \'AOV\' and date_trunc(\'month\',a.date) = b.month union all select \'CR_percent\' as metric, a.date, a.channel_type, a.brand, a.CR_percent value, replace(b.target,\'%\',\'\') daily_target from (select date, upper(channel_type) channel_type, upper(brand) brand, div0(sum(total_orders),sum(sessions)) CR_percent from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type IN (\'CORE\') AND ((lower(marketplace) like (\'%shopify%\') or lower(marketplace) like (\'%cred%\'))) AND brand = \'Bombae\' group by 1,2,3) a left join (select * from bsc_db.maplemonk.daily_revenue_tracker_targets where lower(channel_type) = \'core\' and lower(brand) = \'bombae\') b on b.metric = \'Conversion%\' and date_trunc(\'month\',a.date) = b.month ; create or replace table bsc_db.maplemonk.daily_revenue_tracker_non_core_channel as select m.date, m.channel_aggregated, m.daily_target, n.sales from ( select a.date, b.channel_aggregated, div0(b.target, right(last_day(a.date),2)) daily_target from (select distinct date from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE) a left join (select distinct month, channel_aggregated, replace(target,\',\',\'\') target from bsc_db.maplemonk.daily_revenue_tracker_non_core_channel_targets where metric = \'Sales\' and lower(channel_aggregated) <> \'overall\') b on date_trunc(\'month\', a.date) = b.month ) m left join (select date, source, sum(total_sales_excl_cancl) sales from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where brand = \'Bombay Shaving Company\' and channel_type = \'NON CORE\' group by 1,2 )n on m.date = n.date and lower(m.channel_aggregated) = lower(n.source) ; create or replace table bsc_db.maplemonk.daily_revenue_tracker_non_core_channel_overall as select \'Overall\' as channel_aggregated, a.date, a.brand, a.sales, div0(b.target, right(last_day(a.date),2)) daily_target from (select date, brand, sum(total_sales_excl_cancl) sales from bsc_db.maplemonk.BSC_DB_SALES_COST_SOURCE where channel_type = \'NON CORE\' and brand = \'Bombay Shaving Company\' group by 1,2 ) a left join (select distinct month, channel_aggregated, replace(target,\',\',\'\') target from bsc_db.maplemonk.daily_revenue_tracker_non_core_channel_targets where metric = \'Sales\' and lower(channel_aggregated) = \'overall\' and channel_type = \'Non-Core\') b on date_trunc(\'month\', a.date) = b.month",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from BSC_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            