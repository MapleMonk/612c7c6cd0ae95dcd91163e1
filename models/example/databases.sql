{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.maplemonk.CUSTOMER_RETENTION_BY_PURCHASE_DATE_DRV AS WITH InitialResult AS ( SELECT distinct customer_id_final, last_day(order_date) as order_month, order_date as order_date FROM RPSG_DB.maplemonk.SALES_CONSOLIDATED_DRV where lower(marketplace) like any (\'%shopify%\',\'%woocommerce%\') ), second_time_inmonth AS( select customer_id_final,order_date,order_month,rw from (select customer_id_final, order_date, order_month, row_number() over(partition by customer_id_final,order_month order by order_date ASC ) rw from InitialResult ) where rw=2 ), structuredInitialResult AS ( SELECT distinct customer_id_final, last_day(order_date) as order_month FROM InitialResult ), combineResult AS( select ori.*,date1 from structuredInitialResult ori left join (select customer_id_final,order_month as date1 from structuredInitialResult)dup on ori.customer_id_final = dup.customer_id_final ) select order_month, customer_id_final, max(cm.phone) as phone, max(cm.email) as email, count(distinct customer_id_final) month1, count(distinct case when s.om is not null then customer_id_final end) month2nd_time, count(distinct case when last_day(dateadd(month,1,order_month))=date1 then customer_id_final end) month2, count(distinct case when last_day(dateadd(month,2,order_month))=date1 then customer_id_final end) month3, count(distinct case when last_day(dateadd(month,3,order_month))=date1 then customer_id_final end) month4, count(distinct case when last_day(dateadd(month,4,order_month))=date1 then customer_id_final end) month5, count(distinct case when last_day(dateadd(month,5,order_month))=date1 then customer_id_final end) month6, count(distinct case when last_day(dateadd(month,6,order_month))=date1 then customer_id_final end) month7, count(distinct case when last_day(dateadd(month,7,order_month))=date1 then customer_id_final end) month8, count(distinct case when last_day(dateadd(month,8,order_month))=date1 then customer_id_final end) month9, count(distinct case when last_day(dateadd(month,9,order_month))=date1 then customer_id_final end) month10, count(distinct case when last_day(dateadd(month,10,order_month))=date1 then customer_id_final end) month11, count(distinct case when last_day(dateadd(month,11,order_month))=date1 then customer_id_final end) month12, count(distinct case when last_day(dateadd(month,12,order_month))=date1 then customer_id_final end) month13, count(distinct case when (DATEDIFF(MONTH, last_day(order_month), date1) between 1 and 6) then customer_id_final end) with_in_6_months, count(distinct case when (DATEDIFF(MONTH, last_day(order_month), date1) between 1 and 12) then customer_id_final end) with_in_12_months, count(distinct case when (DATEDIFF(MONTH, last_day(order_month), date1) between 1 and 1000) then customer_id_final end) in_lifetime, count(distinct case when (DATEDIFF(MONTH, last_day(order_month), date1) between 1 and 6) or s.om is not null then customer_id_final end) in_6_months_all, count(distinct case when (DATEDIFF(MONTH, last_day(order_month), date1) between 1 and 12) or s.om is not null then customer_id_final end) in_12_months_all, count(distinct case when (DATEDIFF(MONTH, last_day(order_month), date1) between 1 and 1000) or s.om is not null then customer_id_final end) lifetime_all from combineResult c left join (select customer_id_final as cif,order_month as om from second_time_inmonth) as s on s.cif = c.customer_id_final and s.om= c.order_month left join (select customer_id_final as cid,phone,email from rpsg_db.maplemonk.customer_master_drv) as cm on cm.cid = c.customer_id_final group by 1,2 order by 1 desc",
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
                        