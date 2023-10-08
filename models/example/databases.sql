{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.head_tail_overlap as WITH ProductTags AS ( SELECT REVERSE(SUBSTRING(REVERSE(PARSE_JSON(variants)[0]:sku), CHARINDEX(\'-\', REVERSE(PARSE_JSON(variants)[0]:sku)) + 1)) AS sku_group, value::STRING AS tag FROM snitch_db.maplemonk.SHOPIFYINDIA_products, TABLE(FLATTEN(INPUT => SPLIT(TAGS, \',\'))) ), Heads AS ( SELECT pt.sku_group, pt.tag, available_units as heads_units, final_ros as head_ros, product_name as head_product_name FROM ProductTags pt JOIN snitch_db.maplemonk.availability_master am ON pt.sku_group = am.sku_group WHERE am.sku_class = \'1-Head\' ), TailsAndNew AS ( SELECT pt.sku_group, pt.tag, available_units as tail_units, final_ros as tail_ros, product_name as tail_product_name FROM ProductTags pt JOIN snitch_db.maplemonk.availability_master am ON pt.sku_group = am.sku_group WHERE am.sku_class IN (\'3-Tail\', \'4-New\') ), Overlap AS ( SELECT h.sku_group AS head_sku, tn.sku_group AS tail_new_sku, COUNT(DISTINCT h.tag) AS overlapping_tags_count, head_ros, tail_ros, heads_units, tail_units, head_product_name, tail_product_name FROM Heads h JOIN TailsAndNew tn ON h.tag = tn.tag GROUP BY h.sku_group, tn.sku_group, head_ros, tail_ros, heads_units, tail_units,head_product_name,tail_product_name ) SELECT o.head_sku, o.tail_new_sku, o.head_ros, o.tail_ros, o.heads_units, o.tail_units, head_product_name,tail_product_name, o.overlapping_tags_count FROM Overlap o where upper(left(head_sku,8))<> upper(left(tail_new_sku,8)) and tail_units > 50 and overlapping_tags_count > 15 ORDER BY overlapping_tags_count DESC, o.head_sku, o.tail_new_sku",
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
                        