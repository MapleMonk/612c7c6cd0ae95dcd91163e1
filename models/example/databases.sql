{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table solara_db.maplemonk.Solara_DB_PandL as select coalesce(a.order_date,e.date, f.date, g.date, h.date) as Date ,a.order_id ,a.reference_code ,SALEORDERITEMCODE ,coalesce(a.shop_name,e.marketplace, f.marketplace, g.marketplace, h.marketplace) shop_name ,case when coalesce(a.marketplace,e.marketplace, f.marketplace, g.marketplace, h.marketplace) like \'%MYNTRA%\' then \'MYNTRA SC\' when coalesce(a.marketplace,e.marketplace, f.marketplace, g.marketplace, h.marketplace) = \'AMAZON\' then \'AMAZON SC\' when coalesce(a.marketplace,e.marketplace, f.marketplace, g.marketplace, h.marketplace) like \'%NYKAA%\' then \'NYKAA SC\' when coalesce(a.marketplace,e.marketplace, f.marketplace, g.marketplace, h.marketplace) like \'%FLIPKART%\' then \'FLIPKART SC\' else coalesce(a.marketplace,e.marketplace, f.marketplace, g.marketplace, h.marketplace) end as marketplace ,coalesce(a.source, e.channel, f.channel, g.channel, h.channel) as Marketing_Source ,coalesce(a.channel, e.channel, f.channel, g.channel, h.channel) as Marketing_Channel ,a.return_flag ,a.customer_id_final ,a.new_customer_flag ,payment_mode ,payment_gateway ,a.awb ,a.order_Status ,a.shipping_status ,a.final_shipping_status ,case when lower(order_Status) like \'%cancel% \'or lower(final_shipping_status) like \'%cancel%\' then \'CANCELLED\' else upper(coalesce(final_shipping_status,order_status)) end final_status ,a.sku ,a.sku_code ,a.commonskucode common_sku_code ,a.product_name_final PRODUCT_NAME_FINAL ,a.product_sub_category product_sub_category ,a.product_category PRODUCT_CATEGORY ,a.quantity ,mrp_sales mrp , MRP_sales , mrp_discount ,a.selling_price Gross_sale ,a.shipping_price shipping_price ,case when lower(coalesce(final_status,\'NA\')) not in (\'cancelled\',\'rto\', \'returned\') then ifnull(a.cogs,0)*quantity else 0 end as COGS ,a.tax tax ,div0(ifnull(e.spend,0), count(1) over (partition by coalesce(a.order_date,e.date, f.date, g.date, h.date), coalesce(a.marketplace, e.marketplace, f.marketplace, g.marketplace, h.marketplace),coalesce(a.channel, e.channel, f.channel, g.channel, h.channel))) as Paid_Marketing_Google ,div0(ifnull(f.spend,0), count(1) over (partition by coalesce(a.order_date,e.date, f.date, g.date, h.date) , coalesce(a.marketplace, e.marketplace, f.marketplace, g.marketplace, h.marketplace), coalesce(a.channel, e.channel, f.channel, g.channel, h.channel))) as Paid_Marketing_Facebook ,div0(ifnull(g.spend,0), count(1) over (partition by coalesce(a.order_date,e.date, f.date, g.date, h.date) ,coalesce(a.marketplace, e.marketplace, f.marketplace, g.marketplace, h.marketplace), coalesce(a.channel, e.channel, f.channel, g.channel, h.channel))) as Paid_Marketing_Amazon ,case when a.new_customer_flag = \'Repeat\' then LAG(a.order_date) IGNORE NULLS OVER (partition by a.customer_id_final ORDER BY a.order_date) end previous_date ,datediff(day,previous_date,a.order_Date) days_from_last_order ,case when lower(m_c.commission_type) like \'%percent%\' then m_c.commission_value*ifnull(selling_price,0) when lower(m_c.commission_type) like \'%flat fee for each order%\' then div0(m_c.commission_value,count(1) over (partition by a.reference_code)) when lower(m_c.commission_type) like \'%flat fee for month%\' then div0(m_c.commission_value,count(1) over (partition by a.marketplace, month(a.ORDER_DATE), year(a.order_Date))) end Commission_Fee ,case when lower(l_c.commission_type) like \'%percent%\' then l_c.commission_value*ifnull(selling_price,0) when lower(l_c.commission_type) like \'%flat fee for each order%\' then div0(l_c.commission_value,count(1) over (partition by a.reference_code)) when lower(l_c.commission_type) like \'%flat fee for month%\' then div0(l_c.commission_value,count(1) over (partition by a.marketplace, month(a.ORDER_DATE), year(a.order_Date))) end LOGISTICS_COST ,case when lower(sff.commission_type) like \'%percent%\' then sff.commission_value*ifnull(selling_price,0) when lower(sff.commission_type) like \'%flat fee for each order%\' then div0(sff.commission_value,count(1) over (partition by a.reference_code)) when lower(sff.commission_type) like \'%flat fee for month%\' then div0(sff.commission_value,count(1) over (partition by a.marketplace, month(a.ORDER_DATE), year(a.order_Date))) end SHOPIFY_PLATFORM_FIXED_FEE ,case when lower(svf.commission_type) like \'%percent%\' then svf.commission_value*ifnull(selling_price,0) when lower(svf.commission_type) like \'%flat fee for each order%\' then div0(svf.commission_value,count(1) over (partition by a.reference_code)) when lower(svf.commission_type) like \'%flat fee for month%\' then div0(svf.commission_value,count(1) over (partition by a.marketplace, month(a.ORDER_DATE), year(a.order_Date))) end SHOPIFY_PLATFORM_VARIABLE_FEE from solara_db.maplemonk.Solara_db_sales_consolidated a full outer join (select date , channel , \'SHOPIFY_SOLARA\' MARKETPLACE , sum(spend) spend from solara_db.maplemonk.Solara_db_MARKETING_CONSOLIDATED where lower(channel) like \'%google%\' group by 1,2,3 ) e on e.date = a.order_Date::date and lower(a.marketplace) = \'shopify_solara\' and lower(case when lower(a.channel) like \'%google%\' then \'google\' end) like \'%google%\' full outer join (select date , channel , \'SHOPIFY_SOLARA\' MARKETPLACE , sum(spend) spend from solara_db.maplemonk.Solara_db_MARKETING_CONSOLIDATED where lower(channel) like \'%facebook%\' group by 1,2,3 ) f on f.date = a.order_Date::date and lower(a.marketplace) = \'shopify_solara\' and lower(case when lower(a.channel) like \'%facebook%\' then \'facebook\' end) like \'%facebook%\' full outer join (select date , channel , \'AMAZON VC\' MARKETPLACE , sum(spend) spend from solara_db.maplemonk.Solara_db_MARKETING_CONSOLIDATED where lower(channel) like \'%amazon vc%\' group by 1,2,3 ) g on g.date = a.order_Date::date and lower(a.marketplace) = \'amazon vc\' full outer join (select date , channel , \'AMAZON SC\' MARKETPLACE , sum(spend) spend from solara_db.maplemonk.Solara_db_MARKETING_CONSOLIDATED where lower(channel) like \'%amazon sc%\' group by 1,2,3 ) h on g.date = a.order_Date::date and lower(a.marketplace) = \'amazon sc\' left join (select TO_DATE(END_DATE, \'DD-Mon-YY\')END_DATE, TO_DATE(start_date, \'DD-Mon-YY\')START_DATE, MARKETPLACE, COMMISSION_TYPE, case when lower(commission_type) like \'%percent%\' then div0((replace(COMMISSION_VALUE,\'%\',\'\')::float),100) else COMMISSION_VALUE::int end COMMISSION_VALUE from SOLARA_DB.maplemonk.marketplace_commissions ) m_c on a.order_Date::date >= m_c.START_DATE and a.order_Date::date <= m_c.END_DATE and lower(a.marketplace) = lower(m_c.MARKETPLACE) left join (select TO_DATE(END_DATE, \'DD-Mon-YY\')END_DATE, TO_DATE(START_date, \'DD-Mon-YY\')START_DATE, marketplace, COMMISSION_TYPE, case when lower(commission_type) like \'%percent%\' then div0((replace(COMMISSION_VALUE,\'%\',\'\')::float),100) else COMMISSION_VALUE::int end as COMMISSION_VALUE from SOLARA_DB.maplemonk.other_operation_costs ) l_c on a.order_Date::date >= l_c.START_DATE and a.order_Date::date <= l_c.END_DATE and lower(a.marketplace) = lower(l_c.marketplace) left join (select TO_DATE(TO_DATE, \'DD-Mon-YY\')END_DATE, TO_DATE(FROM_date, \'DD-Mon-YY\')START_DATE, marketplace, DETAIL commission_type, case when lower(detail) like \'%percent%\' then div0((replace(charges,\'%\',\'\')::float),100) else charges::int end as COMMISSION_VALUE from SOLARA_DB.maplemonk.shopify_and_tool_costs where lower(CATEGORY) like \'%fixed_fee%\' ) sff on a.order_Date::date >= sff.START_DATE and a.order_Date::date <= sff.END_DATE and lower(a.marketplace) = lower(sff.marketplace) left join (select TO_DATE(TO_DATE, \'DD-Mon-YY\')END_DATE, TO_DATE(FROM_date, \'DD-Mon-YY\')START_DATE, marketplace, DETAIL commission_type, case when lower(detail) like \'%percent%\' then div0((replace(charges,\'%\',\'\')::float),100) else charges::int end as COMMISSION_VALUE from SOLARA_DB.maplemonk.shopify_and_tool_costs where lower(CATEGORY) like \'%platform%\' ) svf on a.order_Date::date >= svf.START_DATE and a.order_Date::date <= svf.END_DATE and lower(a.marketplace) = lower(svf.marketplace) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SOLARA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        