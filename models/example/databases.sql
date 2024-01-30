{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table snitch_db.maplemonk.ecoreturns_master as with order_data as ( select marketplace_mapped, order_name, order_date, sum(selling_price) as gross_without_tax, sum(tax) as total_tax, sum(mrp) as mrp, sum(cost) as total_cost, count(distinct AWB) as number_of_shipments, count(sku) as number_of_items, sum(return_flag) as number_of_items_returned from snitch_db.maplemonk.unicommerce_availability_merge where marketplace_mapped in (\'SHOPIFY\') group by order_name,marketplace_mapped,order_date order by order_name ), ecoret as ( select *, CASE WHEN CHARINDEX(\' - \', name) > 0 AND CHARINDEX(\' / \', name) > CHARINDEX(\' - \', name) THEN RIGHT(name, CHARINDEX(\' \', REVERSE(name)) - 1) WHEN CHARINDEX(\' - \', name) > 0 THEN LEFT(name, CHARINDEX(\' - \', name) - 1) ELSE \'Unknown\' END as size, CASE WHEN CHARINDEX(\' - \', name) > 0 AND CHARINDEX(\' / \', name) > CHARINDEX(\' - \', name) THEN SUBSTRING(name, CHARINDEX(\' - \', name) + 3, CHARINDEX(\' /\', name) - CHARINDEX(\' - \', name) - 3) WHEN CHARINDEX(\' / \', name) > 0 THEN REVERSE(SUBSTRING(REVERSE(name), 1, CHARINDEX(\' / \', REVERSE(name)) - 1)) ELSE \'Unknown\' END as color, CASE WHEN name LIKE \'%Jeans%\' THEN \'Jeans\' WHEN name LIKE \'%T-Shirt%\' THEN \'T-Shirt\' WHEN name LIKE \'%Shirt%\' AND name NOT LIKE \'%T-Shirt%\' THEN \'Shirt\' WHEN name LIKE \'%Perfume%\' THEN \'Perfume\' WHEN name LIKE \'%Boxer%\' THEN \'Boxer\' WHEN name LIKE \'%Jogsuit%\' THEN \'Jogsuit\' WHEN name LIKE \'%Underpants%\' THEN \'Underpants\' WHEN name LIKE \'%Jogger%\' THEN \'Jogger\' WHEN name LIKE \'%Pyjama%\' THEN \'Pyjama\' WHEN name LIKE \'%Shoes%\' THEN \'Shoes\' WHEN name LIKE \'%Cargo%\' THEN \'Cargo\' WHEN name LIKE \'%Shorts%\' THEN \'Shorts\' WHEN name LIKE \'%Jacket%\' THEN \'Jacket\' WHEN name LIKE \'%Sweatshirt%\' THEN \'Sweatshirt\' WHEN name LIKE \'%Sweater%\' THEN \'Sweater\' WHEN name LIKE \'%Trouser%\' THEN \'Trouser\' WHEN name LIKE \'%Co-Ords%\' THEN \'Co-Ords\' WHEN name LIKE \'%Chino%\' THEN \'Chino\' WHEN name LIKE \'%Denim%\' THEN \'Denim\' ELSE \'Unknown\' END as clothing_type FROM snitch_db.maplemonk.ecoreturns_data ) select * from ecoret left join order_data on ecoret.order_id=order_data.order_name",
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
                        