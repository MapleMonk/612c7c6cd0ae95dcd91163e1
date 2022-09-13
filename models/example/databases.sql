{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rpsg_db.maplemonk.Doctors_cosolidated as select \"DATE\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"STATUS\", \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Akshara\' as Doctor,row_number() over(partition by date,mobile order by date) as pk from rpsg_db.maplemonk.drv_dr_akshara )where pk =1 UNION select \"DATE\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"Status \", \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Surya Bhagwati\' as Doctor,row_number() over(partition by date,mobile order by date) as pk from rpsg_db.maplemonk.drv_dr_surya_bhagwati )where pk =1 UNION select \"DATE\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"STATUS\", \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Sumit\' as Doctor,row_number() over(partition by date,mobile order by date) as pk from rpsg_db.maplemonk.drv_dr__sumit )where pk =1 UNION select \"DATE\", \"DIET\", \"MOBILE\", \"New/FU\", \"SOURCE\", \"STATUS\", \"WA no.\", \"FU Date\", \"REPORTS\", \"COMMENTS\", \"PROBLEMS\", \"PRODUCTS\", \"FU Status\", \"DESCRIPTION\", \"FU Comments\", \"Rx Duration\", \"Extra comments\", \"Customer\'s name\" ,doctor from ( select *,\'Dr. Sneha\' as Doctor,row_number() over(partition by date,mobile order by date) as pk from rpsg_db.maplemonk.drv_dr_sneha )where pk =1; Create or replace table rpsg_db.maplemonk.Appointment_track_drv as select * from( with doctors_cte as ( select * from rpsg_db.maplemonk.doctors_cosolidated where date not like \'%ept%\' and date not like \'%ul%\' and date not like \'%gus%\' and date not like \'%197041%\' and date not like \'Apr%\' ) select * from ( select to_date(d.date,\'dd/MM/yyyy\') as Date ,DIET ,\"Customer\'s name\" as Customer_name ,replace(MOBILE,\' \',\'\') as contact_num ,\"WA no.\" as alternate_contact_num, concat(replace(d.DATE,\'/\',\'\'),replace(mobile,\' \',\'\')) as appointmentid ,doctor ,comments ,\"New/FU\" as DRV_new_customer_flag ,d.status ,source ,\"FU Date\" ,\"FU Status\" ,m.CONSULTED as consulted_flag ,m.RX as prescription_flag ,\"FU Comments\" ,reports ,\"Rx Duration\" ,description ,replace(coalesce(s.GA_CHANNEL,sa.ga_channel),\'NA\',\'Not Available in GA\') as Channel ,coalesce(s.order_id,sa.order_id) as order_id ,coalesce(s.order_status,sa.order_status) as order_status ,coalesce(s.invoice_id,sa.invoice_id) as invoice_id ,coalesce(s.line_item_id,sa.line_item_id) as line_item_id ,coalesce(s.order_date,sa.order_date) as order_date ,coalesce(s.productname,sa.productname) as productname ,coalesce(s.category,sa.category) as category ,coalesce(s.selling_price,sa.selling_price) as selling_price ,coalesce(s.new_customer_flag,sa.new_customer_flag) as MM_new_customer_flag ,coalesce(s.suborder_quantity,sa.suborder_quantity) as suborder_quantity ,coalesce(s.city,sa.city) as city ,coalesce(s.state,sa.state) as state from doctors_cte d left join rpsg_db.maplemonk.sales_consolidated_drv s on d.mobile =s.phone and to_date(d.date,\'dd/MM/yyyy\')<=s.order_date::date and to_date(d.date,\'dd/MM/yyyy\')+30 >=s.order_date::date left join rpsg_db.maplemonk.sales_consolidated_drv sa on d.\"WA no.\" =sa.phone and to_date(d.date,\'dd/MM/yyyy\')<=sa.order_date::date and to_date(d.date,\'dd/MM/yyyy\')+30 >=sa.order_date::date left join rpsg_db.maplemonk.drv_consultation_mapping m on lower((case when \"FU Status\" is null then concat(d.status,\'0\') else concat(d.status,\"FU Status\") end)) = lower(m.conc) )) order by Date desc;",
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
                        