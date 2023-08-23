{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table snitch_db.maplemonk.affiliate_brand_partnership_snitch as SELECT a.order_date, a.order_id, b.order_name, b.phone, b.email, b.customer_flag, b.discount_code, CASE WHEN upper(discount_code) = \'WISHLINK10\' then \'WISHLINK\' WHEN upper(discount_code) = \'WISH20\' then \'WISHLINK\' WHEN upper(discount_code) = \'AD20\' then \'Admitad\' WHEN upper(discount_code) = \'ADM20\' then \'Admitad\' WHEN upper(discount_code) = \'ADMITAD20\' then \'Admitad\' WHEN upper(discount_code) = \'AD10\' then \'Admitad\' WHEN upper(discount_code) = \'ADM10\' then \'Admitad\' WHEN upper(discount_code) = \'ADMITAD10\' then \'Admitad\' WHEN upper(discount_code) = \'KOTAKXS\' then \'Kotak\' WHEN upper(discount_code) = \'STYLEHB\' then \'HDFC\' WHEN upper(discount_code) = \'MASTERCARD20\' then \'Mastercard\' WHEN upper(discount_code) = \'SNITCH999\' then \'Explurger\' WHEN upper(discount_code) = \'STYLE10\' then \'BHIM\' WHEN upper(discount_code) = \'RUPAY300\' then \'Rupay\' WHEN upper(discount_code) = \'SNITCH10\' then \'Kickcash\' WHEN upper(discount_code) = \'DISCOVER25\' then \'Kickcash\' WHEN upper(discount_code) = \'UPI300\' then \'BHIM\' WHEN upper(discount_code) = \'UISTY\' then \'Swop store\' WHEN upper(discount_code) = \'GBIFD\' then \'Swop store\' WHEN upper(discount_code) = \'PPAX20\' then \'Axis bank\' WHEN upper(discount_code) = \'FEDERAL500\' then \'Federal Bank\' WHEN upper(discount_code) = \'FEDERAL15\' then \'Federal Bank\' WHEN upper(discount_code) = \'GAMEON\' then \'Ballebaazi\' WHEN upper(discount_code) = \'GAMEON200\' then \'Ballebaazi\' WHEN upper(discount_code) = \'VIPP20\' then \'VI\' WHEN upper(discount_code) = \'VIPP300\' then \'VI\' WHEN upper(discount_code) = \'HDPP20\' then \'HDFC\' WHEN upper(discount_code) = \'HDPP300\' then \'HDFC\' WHEN upper(discount_code) = \'GBPP20\' then \'Grabon\' WHEN upper(discount_code) = \'GBPP300\' then \'Grabon\' WHEN upper(discount_code) = \'GRAB20\' then \'Grabon\' WHEN upper(discount_code) = \'GRAB300\' then \'Grabon\' WHEN upper(discount_code) = \'TPLAYPP20\' then \'Tata Play\' WHEN upper(discount_code) = \'TPLAYPP300\' then \'Tata Play\' WHEN lower(discount_code) = \'gppf4zrb5zrg\' then \'Intermiles\' WHEN upper(discount_code) LIKE \'%STY-%\' then \'CheQ\' WHEN upper(discount_code) LIKE \'%SCH-%\' then \'CheQ\' WHEN upper(discount_code) LIKE \'%CHR-%\' then \'Chroma\' WHEN upper(discount_code) = \'CRED25\' then \'Cred\' WHEN upper(discount_code) LIKE \'%SWP-%\' then \'SwopStore\' WHEN upper(discount_code) = \'NIYOX\' then \'NiyoX\' end as partner, SUM(b.discount) AS discount, SUM(suborder_quantity) AS quantity, SUM(selling_price) AS sales, SUM(CASE WHEN a.return_quantity <> 0 AND cancelled_quantity = 0 THEN selling_price END) AS return_sales, SUM(CASE WHEN cancelled_quantity <> 0 THEN selling_price END) AS cancelled_sales, CASE WHEN SUM(a.return_quantity) > 0 AND SUM(a.return_quantity) < SUM(suborder_quantity) THEN \'Partially Returned\' WHEN SUM(a.return_quantity) > 0 AND SUM(a.return_quantity) = SUM(suborder_quantity) THEN \'Fully Returned\' WHEN SUM(a.return_quantity) = 0 THEN \'Not Returned\' END AS return_status, CASE WHEN SUM(cancelled_quantity) > 0 AND SUM(cancelled_quantity) < SUM(suborder_quantity) THEN \'Partially Cancelled\' WHEN SUM(cancelled_quantity) > 0 AND SUM(cancelled_quantity) = SUM(suborder_quantity) THEN \'Fully Cancelled\' WHEN SUM(cancelled_quantity) = 0 THEN \'Not Cancelled\' END AS cancelled_status FROM snitch_db.maplemonk.fact_items_snitch b LEFT JOIN ( SELECT order_date, order_id, suborder_quantity, selling_price, return_quantity, cancelled_quantity, saleorderitemcode FROM snitch_db.maplemonk.unicommerce_fact_items_snitch) a ON a.order_id = b.order_id and b.line_item_id=split_part(a.saleorderitemcode,\'-\',0) WHERE partner IN (\'WISHLINK\',\'Admitad\',\'Kotak\',\'HDFC\',\'Mastercard\',\'Explurger\',\'BHIM\',\'Rupay\',\'Kickcash\',\'Swop store\',\'Axis bank\',\'Federal Bank\',\'Ballebaazi\',\'VI\',\'HDFC\',\'Grabon\',\'Tata Play\',\'Intermiles\',\'CheQ\',\'Chroma\',\'Cred\') AND order_name is not null GROUP BY 1,2,3,4,5,6,7 order by sales desc",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        