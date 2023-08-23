{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table indusvalley_db.MAPLEMONK.indusvalley_db_sales_Pacing as with category_daily_sales as ( select order_date::date Date ,upper(product_category_1) product_category_1 ,upper(product_category_2) product_category_2 ,shop_name ,sum(ifnull(selling_price,0)) Daily_sales ,sum(ifnull(quantity,0)) DAILY_QUANTITY ,sum(ifnull(SELLING_PRICE, 0)) * div0( (datediff(day,min(date_trunc(\'month\',order_date::date)),max(last_day(order_date::date)))+1) ,datediff(day,min(date_trunc(\'month\',order_date::date)), MAX(MAX(order_date::date)) OVER (PARTITION BY date_trunc(\'month\', order_date::date)) ) +1 ) Sales_Pacing from indusvalley_db.MAPLEMONK.indusvalley_db_sales_consolidated group by 1,2,3,4 ), CALENDAR_CAT_DIM as ( SELECT Dim.Date ,categories.shop_name ,categories.product_category_1 ,categories.product_category_2 FROM ( (SELECT DATEADD(DAY, SEQ4(), \'2016-01-01\')::date AS DATE FROM TABLE(GENERATOR(ROWCOUNT=>4000)) ) DIM cross join (SELECT distinct shop_name ,product_category_1 , product_category_2 FROM indusvalley_db.MAPLEMONK.indusvalley_db_sales_consolidated ) categories ) ), Target_Sales as ( Select * from ( SELECT try_to_date(month,\'mm/dd/yyyy\') as month ,upper(category_1) category_1 ,upper(category_2) category_2 ,try_to_double(replace(monthly_target,\',\',\'\'))::int target ,row_number() over (partition by upper(category_1), upper(category_2) order by 1) rw from indusvalley_db.MAPLEMONK.target_category_sales ) where rw=1 ), Cat1Sales AS ( SELECT upper(product_category_1) product_category_1, DATE_TRUNC(\'MONTH\', order_date) AS Month, SUM(IFNULL(selling_price, 0)) AS overall_sales, sum(ifnull(quantity,0)) overall_quantity, sum(case when lower(shop_name) like \'%shopify%\' then ifnull(selling_price,0) end) sales_shopify, sum(case when lower(shop_name) like \'%amazon%\' then ifnull(selling_price,0) end) sales_amazon, sum(case when lower(shop_name) like \'%shopify%\' then ifnull(quantity,0) end) quantity_shopify, sum(case when lower(shop_name) like \'%amazon%\' then ifnull(quantity,0) end) quantity_amazon FROM indusvalley_db.MAPLEMONK.indusvalley_db_sales_consolidated GROUP BY 1, 2 ), Cat1LTMaxSales AS ( SELECT product_category_1, MAX(ifnull(overall_sales,0)) max_overall_sales, MAX(ifnull(overall_quantity,0)) max_overall_quantity, MAX(ifnull(sales_shopify,0)) AS max_shop_sales, MAX(ifnull(quantity_shopify,0)) max_shop_quantity, MAX(ifnull(sales_amazon,0)) AS max_amazon_sales, MAX(ifnull(quantity_amazon,0)) max_amazon_quantity FROM Cat1Sales GROUP BY 1 ), Cat2Sales AS ( SELECT upper(product_category_1) product_category_1, upper(product_category_2) product_category_2, DATE_TRUNC(\'MONTH\', order_date) AS Month, SUM(IFNULL(selling_price, 0)) AS overall_sales, sum(ifnull(quantity,0)) overall_quantity, sum(case when lower(shop_name) like \'%shopify%\' then ifnull(selling_price,0) end) sales_shopify, sum(case when lower(shop_name) like \'%amazon%\' then ifnull(selling_price,0) end) sales_amazon, sum(case when lower(shop_name) like \'%shopify%\' then ifnull(quantity,0) end) quantity_shopify, sum(case when lower(shop_name) like \'%amazon%\' then ifnull(quantity,0) end) quantity_amazon FROM indusvalley_db.MAPLEMONK.indusvalley_db_sales_consolidated GROUP BY 1, 2, 3 ), Cat2LTMaxSales AS ( SELECT product_category_1, product_category_2, MAX(ifnull(overall_sales,0)) max_overall_sales, MAX(ifnull(overall_quantity,0)) max_overall_quantity, MAX(ifnull(sales_shopify,0)) AS max_shop_sales, MAX(ifnull(quantity_shopify,0)) max_shop_quantity, MAX(ifnull(sales_amazon,0)) AS max_amazon_sales, MAX(ifnull(quantity_amazon,0)) max_amazon_quantity FROM Cat2Sales GROUP BY 1, 2 ) SELECT CCD.product_category_1, CCD.product_category_2, CCD.shop_name, CCD.date, date_trunc(\'month\',CCD.date) month_start, div0(TS.target,datediff(day,date_trunc(\'month\',CCD.date),last_day(CCD.date))+1) Target, ifnull(ds.Sales_Pacing,0) sales_pacing, ifnull(ds.daily_sales,0) daily_sales, ifnull(ds.DAILY_QUANTITY,0) daily_quantity, sum(ifnull(ds.daily_sales,0)) over (partition by CCD.product_category_1, CCD.date) sales_cat_1, sum(ifnull(ds.daily_sales,0)) over (partition by CCD.product_category_2, CCD.date) sales_cat_2, sum(ifnull(ds.daily_sales,0)) over (partition by CCD.product_category_1, CCD.product_Category_2, CCD.date) sales_cat_1_2, sum(ifnull(ds.daily_sales,0)) over (partition by CCD.date) overall_daily_sales, cat1.max_overall_sales Cat1_Max_Overall_Sales, cat1.max_shop_sales Cat1_Max_Shopify_Sales, cat1.max_amazon_sales Cat1_Max_Amazon_Sales, cat1.max_overall_quantity Cat1_Max_Overall_quantity, cat1.max_shop_quantity Cat1_Max_Shopify_quantity, cat1.max_amazon_quantity Cat1_Max_Amazon_quantity, cat2.max_overall_sales Cat2_Max_Overall_Sales, cat2.max_shop_sales Cat2_Max_Shopify_Sales, cat2.max_amazon_sales Cat2_Max_Amazon_Sales, cat2.max_overall_quantity Cat2_Max_Overall_quantity, cat2.max_shop_quantity Cat2_Max_Shopify_quantity, cat2.max_amazon_quantity Cat2_Max_Amazon_quantity FROM CALENDAR_CAT_DIM CCD LEFT JOIN category_daily_sales ds ON coalesce(CCD.product_category_1,\'1\') = coalesce(ds.product_category_1,\'1\') and coalesce(CCD.product_category_2,\'1\') = coalesce(ds.product_category_2,\'1\') and CCD.date = ds.date and CCD.shop_name = ds.shop_name LEFT JOIN Target_Sales TS ON coalesce(CCD.product_category_1,\'1\') = coalesce(TS.CATEGORY_1,\'1\') and coalesce(CCD.product_category_2,\'1\') = coalesce(TS.CATEGORY_2,\'1\') and date_trunc(\'month\',CCD.date) = TS.month and lower(CCD.shop_name) like \'%shopify%\' LEFT JOIN Cat1LTMaxSales cat1 ON coalesce(CCD.product_category_1,\'1\') = coalesce(cat1.product_category_1,\'1\') LEFT JOIN Cat2LTMaxSales cat2 ON coalesce(CCD.product_category_1,\'1\') = coalesce(cat2.product_category_1,\'1\') and coalesce(CCD.product_category_2,\'1\') = coalesce(cat2.product_category_2,\'1\') where CCD.date <= current_date;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from indusvalley_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        