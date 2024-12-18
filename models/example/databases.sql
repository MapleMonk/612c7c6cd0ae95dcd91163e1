{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace table Maplemonk.cc_affiliate_Fact_Items as With Affiliate_Sales as ( select om.* ,aff.aff_net_name ,order_network ,safe_divide(ifnull(selling_price,0), count(*) over(partition by order_id,brand,region)) as Calculated_Sales from ( select order_id, order_date, brand, region, sum(ifnull(selling_price,0))selling_price from `MAPLEMONK.CC_sales_consolidated` where Refund_Amount is null and order_id is not null group by 1,2,3,4 )om left join ( select order_id af_order_id,aff_net_name,order_network from cc_comfort_db.affiliates_network_sales_track_data s left join ( select distinct aff_net_id,aff_net_name from cc_comfort_db.affiliates_network )m on cast(s.order_network as int64) = (cast(m.aff_net_id as int64)) where lower(ifnull(order_status,\'\')) in (\'approved\',\'pending\') )aff on casT(aff.af_order_id as string) = om.order_id where af_order_id is not null ) select coalesce(order_date,last_day_of_month) Date, order_id, brand, region, aff_net_name, coalesce(order_network,network_id) network_id, Calculated_Sales as selling_price, safe_divide(Calculated_Sales,sum(Calculated_Sales) over(partition by last_day(order_date),order_network)) share, ifnull(cost_value,0) * (safe_divide(Calculated_Sales,sum(Calculated_Sales) over(partition by last_day(order_date),order_network))) as Affiliate_Spend from Affiliate_Sales fi full outer join ( select network_id ,LAST_DAY(DATE(cast(year_number as int64), cast(financial_month as int64), 1)) AS last_day_of_month ,sum(ifnull(cost_value,0)) cost_value from cc_comfort_db.amr_network_cost_data group by 1,2 )ncd on last_day(fi.order_date) = ncd.last_day_of_month and fi.order_network = ncd.network_id",
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
            