{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.inventory_delta as (with gql as (SELECT DISTINCT CASE WHEN REGEXP_COUNT(var.value:\"sku\"::string, \'-\') >= 2 THEN LEFT(var.value:\"sku\"::string, LENGTH(var.value:\"sku\"::string) - POSITION(\'-\' IN REVERSE(var.value:\"sku\"::string))) ELSE var.value:\"sku\"::string END AS sku_group, t.title AS product_title, var.value:\"price\"::int AS selling_price, var.value:\"compareAtPrice\"::int AS mrp FROM snitch_db.maplemonk.new_meafields_product_products_graph_ql t, LATERAL FLATTEN(input => PARSE_JSON(t.variants)) var ), base as ( select DISTINCT trim(upper(REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))))) AS sku_group, ifnull(sum(case when date = current_date then inventory end),0) as current_date_inv, ifnull(sum(case when date = current_date-1 then inventory end),0) as last_date_inv, ifnull(sum(case when date = current_date then inventory end),0) - ifnull(sum(case when date = current_date-1 then inventory end),0) as inv_delta from snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE facility in (\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL-NORTH-TAURU\') group by 1 ) select base.*, gql.selling_price, gql.mrp from base left join gql on base.sku_group=gql.sku_group)",
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
            