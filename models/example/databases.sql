{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.product_growth as with orders_data as ( select marketplace, order_Date, ifnull(product_category, \'NA\') product_category, sum(case when (lower(ifnull(ORDER_STATUS,\'\')) not like \'%cancel%\' and lower(ifnull(FINAL_SHIPPING_STATUS,\'\')) not like \'%cancel%\') and ifnull(return_flag,0) <> 1 then ifnull(selling_price,0) end) net_Sales, from maplemonk.neon_poetry_482906_j7_sales_consolidated group by 1,2,3 ), base AS ( SELECT order_date, marketplace, product_category, net_sales, SUM(net_sales) OVER ( PARTITION BY product_category, marketplace, DATE_TRUNC(order_date, MONTH) ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS MTD_net_sales FROM orders_data ) SELECT a.order_date, a.product_category, a.marketplace, a.net_sales, a.MTD_net_sales, b.MTD_net_sales AS LMTD_net_sales FROM base a LEFT JOIN base b ON a.product_category = b.product_category AND a.marketplace = b.marketplace AND b.order_date = DATE_SUB(a.order_date, INTERVAL 1 MONTH) ORDER BY 1,2,3 ; create or replace table maplemonk.geo_expansion as with orders_data as ( select marketplace, order_Date, ifnull(state, \'NA\') state, ifnull(product_category, \'NA\') product_category, sum(case when (lower(ifnull(ORDER_STATUS,\'\')) not like \'%cancel%\' and lower(ifnull(FINAL_SHIPPING_STATUS,\'\')) not like \'%cancel%\') and ifnull(return_flag,0) <> 1 then ifnull(selling_price,0) end) net_Sales, from maplemonk.neon_poetry_482906_j7_sales_consolidated group by 1,2,3,4 ), base AS ( SELECT order_date, marketplace, state, product_category, net_sales, SUM(net_sales) OVER ( PARTITION BY marketplace, state, product_category ORDER BY UNIX_DATE(order_date) RANGE BETWEEN 6 PRECEDING AND CURRENT ROW ) AS L7D_net_sales FROM orders_Data ) SELECT a.order_date, a.marketplace, a.state, a.product_category, a.net_sales, a.L7D_net_sales, b.L7D_net_sales AS LY_L7D_net_sales FROM base a LEFT JOIN base b ON a.marketplace = b.marketplace AND a.state = b.state and a.product_category = b.product_category AND b.order_date = DATE_SUB(a.order_date, INTERVAL 1 YEAR) ORDER BY 1,2,3;",
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
            