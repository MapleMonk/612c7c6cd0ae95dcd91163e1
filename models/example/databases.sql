{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rpsg_db.maplemonk.Doctors_cosolidated as select \"Final Date\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"STATUS\" STATUS, \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Akshara\' as Doctor,row_number() over(partition by to_date(\"Final Date\",\'MM/dd/YYYY\'),mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\')) as pk from rpsg_db.maplemonk.drv_dr_akshara )where pk =1 UNION select \"Final Date\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"STATUS\" STATUS, \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name \" ,doctor from ( select *,\'Dr. Surya Bhagwati\' as Doctor,row_number() over(partition by to_date(\"Final Date\",\'MM/dd/YYYY\') ,mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\')) as pk from rpsg_db.maplemonk.drv_dr_surya_bhagwati )where pk =1 UNION select \"Final Date\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"STATUS\" STATUS, \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Sumit\' as Doctor,row_number() over(partition by to_date(\"Final Date\",\'MM/dd/YYYY\') ,mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\')) as pk from rpsg_db.maplemonk.drv_dr__sumit )where pk =1 UNION select \"Final Date\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"STATUS\" STATUS, \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Sneha\' as Doctor,row_number() over(partition by to_date(\"Final Date\",\'MM/dd/YYYY\'),mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\')) as pk from rpsg_db.maplemonk.drv_dr_sneha )where pk =1 union select \"Final Date\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"Status \" STATUS, \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Agnivesh\' as Doctor,row_number() over(partition by to_date(\"Final Date\",\'MM/dd/YYYY\'),mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\')) as pk from rpsg_db.maplemonk.drv_dr_agnivesh )where pk =1 union select \"Final Date\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"Status \" STATUS, \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Sneha Shelar\' as Doctor,row_number() over(partition by to_date(\"Final Date\",\'MM/dd/YYYY\'),mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\')) as pk from rpsg_db.maplemonk.drv_dr_sneha_shelar )where pk =1 ; Create or replace table rpsg_db.maplemonk.Appointment_track_drv as select * from ( select * from ( with final_doc_cte as ( with doctors_cte as ( with cte2 as ( with cte as ( select * from rpsg_db.maplemonk.doctors_cosolidated where \"Final Date\" not like \'%ept%\' and \"Final Date\" not like \'%ul%\' and \"Final Date\" not like \'%gus%\' and \"Final Date\" not like \'%197041%\' and \"Final Date\" not like \'Apr%\' and mobile is not null ) select *,row_number() over(partition by mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\') desc) rw from cte ) select a.*,b.\"Final Date\" as next_date from cte2 a left join cte2 b on a.mobile = b.mobile and a.rw=b.rw+1 order by \"Final Date\" desc ) ,max_date as( select distinct order_Date::date as purchase_date ,phone from rpsg_db.maplemonk.sales_consolidated_drv ) ,inter_cte as ( select d.\"Final Date\" ,d.mobile ,x.purchase_date as last_purchase_date ,row_number() over(partition by d.mobile,to_date(d.\"Final Date\",\'MM/dd/yyyy\') order by x.purchase_date desc) as ret_rw from doctors_cte d left join max_date x on replace(d.MOBILE,\' \',\'\') = phone and to_date(d.\"Final Date\",\'MM/dd/yyyy\') > x.purchase_date and to_date(d.\"Final Date\",\'MM/dd/yyyy\') - 30 < x.purchase_date ) select * from ( select to_date(d.\"Final Date\",\'MM/DD/YYYY\') as Date ,DIET ,\"Customer\'s name\"::varchar as Customer_name ,replace(d.MOBILE,\' \',\'\')::varchar as contact_num ,\"WA no.\"::varchar as alternate_contact_num, concat(replace(d.\"Final Date\",\'/\',\'\'),replace(d.mobile,\' \',\'\'))::varchar as appointmentid ,doctor ,comments::varchar as comments ,\"New/FU\"::varchar as \"New/FU\" ,d.status::varchar as status ,source::varchar as source ,\"FU Date\" ,\"FU Status\" ,m.CONSULTED::varchar as consulted_flag ,m.RX::varchar as prescription_flag ,\"FU Comments\" ,reports::varchar as reports ,\"Rx Duration\" ,description::varchar as description ,i.last_purchase_date as last_purchase_date ,d.next_date as next_date from doctors_cte d left join rpsg_db.maplemonk.drv_consultation_mapping m on lower((case when \"FU Status\" is null then concat(d.status,\'0\') else concat(d.status,\"FU Status\") end)) = lower(m.conc) left join inter_cte i on d.\"Final Date\" = i.\"Final Date\" and d.mobile = i.mobile where ret_rw = 1 ) ) select fd.* ,case when last_purchase_date is null then \'New\' when last_purchase_date is not null and fd.description like \'%ost%\' then \'New\' else \'Repeat\' end as Custom_Retention_flag ,replace(coalesce(s.GA_CHANNEL,null),\'NA\',\'Not Available in GA\') as Channel ,coalesce(s.order_id,null) as order_id ,coalesce(s.order_status,null) as order_status ,coalesce(s.invoice_id,null) as invoice_id ,coalesce(s.line_item_id,null) as line_item_id ,coalesce(s.order_date,null) as order_date ,coalesce(s.product_name_mapped,null) as productname ,coalesce(s.category,null) as category ,coalesce(s.selling_price,null) as selling_price ,coalesce(s.new_customer_flag,null) as MM_new_customer_flag ,coalesce(s.suborder_quantity,null) as suborder_quantity ,coalesce(s.city,null) as city ,coalesce(s.state,null) as state from final_doc_cte fd left join rpsg_db.maplemonk.sales_consolidated_drv s on fd.contact_num =replace(s.phone,\'+91\',\'\') and ( case when next_date is null then (fd.Date<=s.order_date::date and dateadd(month,6,fd.date) >=s.order_date::date) else (fd.date<=s.order_date::date and to_date(fd.next_date)>s.order_Date::date) end) ) order by date desc ) union select * from ( select * from( with final_doc_cte as ( with doctors_cte as ( with cte2 as ( with cte as ( select * from rpsg_db.maplemonk.doctors_cosolidated where \"Final Date\" not like \'%ept%\' and \"Final Date\" not like \'%ul%\' and \"Final Date\" not like \'%gus%\' and \"Final Date\" not like \'%197041%\' and \"Final Date\" not like \'Apr%\' and mobile is not null ) select *,row_number() over(partition by mobile order by to_date(\"Final Date\",\'MM/dd/YYYY\') desc) rw from cte ) select a.*,b.\"Final Date\" as next_date from cte2 a left join cte2 b on a.mobile = b.mobile and a.rw=b.rw+1 order by \"Final Date\" desc ) ,max_date as ( select distinct order_Date::date as purchase_date,phone from rpsg_db.maplemonk.sales_consolidated_drv ) ,inter_cte as ( select d.\"Final Date\",d.mobile,x.purchase_date as last_purchase_date ,row_number() over(partition by d.mobile,to_date(d.\"Final Date\",\'MM/dd/yyyy\') order by x.purchase_date desc) as ret_rw from doctors_cte d left join max_date x on replace(d.MOBILE,\' \',\'\') = phone and to_date(d.\"Final Date\",\'MM/dd/yyyy\') > x.purchase_date and to_date(d.\"Final Date\",\'MM/dd/yyyy\')- 30 < x.purchase_date ) select * from ( select to_date(d.\"Final Date\",\'MM/DD/YYYY\') as Date ,DIET ,\"Customer\'s name\"::varchar as Customer_name ,replace(d.MOBILE,\' \',\'\')::varchar as contact_num ,\"WA no.\"::varchar as alternate_contact_num, concat(replace(d.\"Final Date\",\'/\',\'\'),replace(d.mobile,\' \',\'\'))::varchar as appointmentid ,doctor ,comments::varchar as comments ,\"New/FU\"::varchar as \"New/FU\" ,d.status::varchar as status ,source::varchar as source ,\"FU Date\" ,\"FU Status\" ,m.CONSULTED::varchar as consulted_flag ,m.RX::varchar as prescription_flag ,\"FU Comments\" ,reports::varchar as reports ,\"Rx Duration\" ,description::varchar as description ,i.last_purchase_date as last_purchase_date ,d.next_date as next_date from doctors_cte d left join rpsg_db.maplemonk.drv_consultation_mapping m on lower((case when \"FU Status\" is null then concat(d.status,\'0\') else concat(d.status,\"FU Status\") end)) = lower(m.conc) left join inter_cte i on d.\"Final Date\" = i.\"Final Date\" and d.mobile = i.mobile where ret_rw = 1) ) select fd.* ,case when last_purchase_date is null then \'New\' when last_purchase_date is not null and fd.description like \'%ost%\' then \'New\' else \'Repeat\' end as Custom_Retention_flag ,replace(coalesce(sa.GA_CHANNEL,null),\'NA\',\'Not Available in GA\') as Channel ,coalesce(sa.order_id,null) as order_id ,coalesce(sa.order_status,null) as order_status ,coalesce(sa.invoice_id,null) as invoice_id ,coalesce(sa.line_item_id,null) as line_item_id ,coalesce(sa.order_date,null) as order_date ,coalesce(sa.product_name_mapped,null) as productname ,coalesce(sa.category,null) as category ,coalesce(sa.selling_price,null) as selling_price ,coalesce(sa.new_customer_flag,null) as MM_new_customer_flag ,coalesce(sa.suborder_quantity,null) as suborder_quantity ,coalesce(sa.city,null) as city ,coalesce(sa.state,null) as state from final_doc_cte fd left join rpsg_db.maplemonk.sales_consolidated_drv sa on (case when fd.alternate_contact_num is not null then fd.alternate_contact_num end) =replace(sa.phone,\'+91\',\'\') and ( case when next_date is null then (fd.Date<=sa.order_date::date and dateadd(month,6,fd.date) >=sa.order_date::date) else (fd.date<=sa.order_date::date and to_date(fd.next_date)>sa.order_Date::date) end)) order by date desc ) where order_date is not null; create or replace table rpsg_db.maplemonk.appointments_month_on_month as with cte as ( select date_trunc(\'month\',date) month_start,doctor ,div0(ifnull(count(distinct appointmentid),0),case when date_trunc(\'month\',date) = date_trunc(\'month\',getdate()::Date) then right(getdate()::date - 1,2) else right(last_day(date_trunc(\'month\',date)),2) end) as appointments ,div0(ifnull(count(case when lower(consulted_flag) = \'consulted\' then appointmentid end),0),case when date_trunc(\'month\',date) = date_trunc(\'month\',getdate()::Date) then right(getdate()::date - 1,2) else right(last_day(date_trunc(\'month\',date)),2) end) as Consultations ,div0(ifnull(count(case when lower(prescription_flag) = \'rx\' then appointmentid end),0),case when date_trunc(\'month\',date) = date_trunc(\'month\',getdate()::Date) then right(getdate()::date - 1,2) else right(last_day(date_trunc(\'month\',date)),2) end) as Prescriptions ,div0(ifnull(count(distinct case when lower(consulted_flag) = \'consulted\' and order_id is not null then appointmentid end),0),case when date_trunc(\'month\',date) = date_trunc(\'month\',getdate()::Date) then right(getdate()::date - 1,2) else right(last_day(date_trunc(\'month\',date)),2) end) as Appointments_to_Orders_Booked ,div0(ifnull(sum( case when lower(consulted_flag) = \'consulted\' and order_id is not null then selling_price end),0),case when date_trunc(\'month\',date) = date_trunc(\'month\',getdate()::Date) then right(getdate()::date - 1,2) else right(last_day(date_trunc(\'month\',date)),2) end) as Appointments_Booked_to_revenue from rpsg_db.maplemonk.appointment_track_drv group by 1,2 order by 1,2 ) select a.*, ifnull(b.appointments,0) prev_month_appointments, ifnull(b.consultations,0) prev_month_consultations, ifnull(b.prescriptions,0) prev_month_prescriptions, ifnull(b.Appointments_to_Orders_Booked,0) prev_month_Appointments_to_Orders_Booked, ifnull(b.Appointments_Booked_to_revenue,0) prev_month_Appointments_Booked_to_revenue from cte a left join cte b on a.month_start = dateadd(month,1,b.month_start) and a.doctor= b.doctor; create or replace table rpsg_db.maplemonk.DRV_INT_Practo_track as with international_cte as ( select to_date(date,\'dd/MM/YYYY\') as date,\"Dr Name\" as doctor,count(distinct concat(replace(date,\'/\',\'\'),replace(mobile,\' \',\'\'))) as orders, sum(replace(rev,\',\',\'\')::float) as International_revenue from( select *, row_number() over(partition by date,mobile,rev order by date desc)from rpsg_db.maplemonk.drv_doctors_int where \"Consult/Order\" = \'Order\') group by 1,2 order by 1,2 desc ), practo_cte as ( select to_date(date,\'dd/MM/YYYY\') as date ,sum(replace(replace(revenue,\',\',\'\'),\'-\',\'0\')::float) as Practo_revenue from(select *,row_number() over(partition by date,revenue order by date desc) rw from rpsg_db.maplemonk.drv_doctors_practo) where rw =1 group by 1 order by 1 desc )select p.date,p.practo_revenue/count(1) over(partition by p.date) as practo_revenue,i.orders as international_orders,i.international_revenue,i.doctor from practo_cte p left join international_cte i on p.date = i.date order by 1 desc;",
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
                        