{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table xyxx_db.maplemonk.gross_margin_xyxx as select * from ( with GT_CTE as ( select f.* ,replace(g.\"GM%\",\'%\',\'\')::float as \"GM%\" ,(f.grossvalueperitem * (ifnull(replace(g.\"GM%\",\'%\',\'\')::float,0)/100)) as GROSSMARGINVALUE ,g.channel ,g.range from xyxx_db.maplemonk.fieldassist_outlet_employee_date_xyxx f left join xyxx_db.maplemonk.googlesheet_maplemonk_gm g on lower(f.outletchannelname) = lower(g.channel) and month(f.date) = month(to_date(g.month,\'Mon/YYYY\')) and year(f.date) = year(to_date(g.month,\'Mon/YYYY\')) and case when length(displaycategory)<4 then lower(displaycategory) else lower(left(displaycategory,charindex(\' \',displaycategory)-1)) end = lower(g.range) and lower(f.category)=lower(g.category) ) , SALES_CTE as ( select f.* from xyxx_db.maplemonk.sales_consolidated_xyxx f left join (select * from xyxx_db.maplemonk.googlesheet_maplemonk_gm where channel <> \'GT\') g on month(f.order_date::date) = month(to_date(g.month,\'Mon/YYYY\')) and year(f.order_date::date) = year(to_date(g.month,\'Mon/YYYY\')) and f.range = g.range and lower(f.category)=lower(g.category) ) select date as Date ,productname as PRODUCT_NAME ,range ,category ,\'Field Assist\' as Marketplace ,\'GT\' as sales_channel ,count(case when productive = \'true\' then visitid end) as Orders ,sum(quantityperitem) as total_quantity ,sum(grossvalueperitem) as total_grosssales ,\"GM%\" ,sum(grossmarginvalue) as total_grossmarginvalue from GT_CTE group by Date, product_name,range,category,Marketplace,sales_channel,\"GM%\" UNION SELECT order_date:: date as Date ,PRODUCT_NAME ,range ,category ,shop_name as Marketplace ,\'Portals / Website\' as sales_channel ,count(distinct case when lower(order_status) <> \'cancelled\' then order_id end) as Orders ,sum(quantity) as total_quantity ,sum(selling_price) as total_grosssales ,\"GM%\" ,sum(grossmarginvalue) as total_grossmarginvalue from sales_cte group by Date, product_name,range,category,Marketplace,sales_channel,\"GM%\" order by Date desc ) ; create or replace table xyxx_db.maplemonk.gross_margin_xyxx as select * ,case when sales_channel = \'GT\' then \'GT\' when sales_channel <> \'GT\' and marketplace like \'%hopify%\' then \'Website\' else \'Portal\' end as SALES_CHANNEL_L1 from xyxx_db.maplemonk.gross_margin_xyxx;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        