{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace Table SLEEPYCAT_DB.maplemonk.Standard_vs_Custom_Discount as select sc.* ,upper(coalesce(dw.Subcategory,\'others\')) final_product_sub_category ,\"Discount %\" as standard_Discount , case when ifnull(standard_Discount,0) - (div0((ifnull(total_mrp, 0)) - (ifnull(selling_price,0)) ,(ifnull(total_mrp, 0)))*100) >= 0 then \'OK\' else \'Excess Discount\' end as Remark from ( select * from sleepycat_db.maplemonk.sleepycat_db_sales_consolidated where lower(type) = \'sales\' )sc left join ( select * from ( select * ,row_number() over(partition by SKU_CODE ,start_date,end_Date order by 1)rw from sleepycat_db.maplemonk.sleepycat_db_sheet1 ) where rw = 1 )dw on order_date ::date >= start_date::date and order_date::date <= end_date::date and lower(sc.SKU) = lower(dw.SKU_CODE)",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SLEEPYCAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            