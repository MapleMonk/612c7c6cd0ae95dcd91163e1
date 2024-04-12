{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table snitch_db.maplemonk.Str_calculation as With Day_level_data as ( select inv.date, inv.sku_group, inv.available_units, inv.price_with_tax, inv.price_without_tax, inv.Unit_price_with_tax, inv.Unit_price_without_tax, ifnull(ufi.sold_quantity,0) as sold_quantity, ufi.SUBORDER_QUANTITY, ufi.SHIPPING_QUANTITY, ifnull(ufi.SOLD_AT_MRP,0) as SOLD_AT_MRP, ifnull(ufi.SOLD_AT_DISCOUNT,0) as SOLD_AT_DISCOUNT, ifnull(ufi.SALES,0) as SALES, ifnull(ufi.DISCOUNT,0) as DISCOUNT, ifnull(ufi.mrp_sales,0) as mrp_sales, --ABCD.Price, ifnull(scm.units_inverted,0) as units_inwarded, uam.sleeve_type, uam.collar_type, uam.CATEGORY, uam.FABRIC, uam.hem, uam.design, uam.closure, uam.fit, uam.occassion, uam.color from ( select _airbyte_emitted_at ::date::string::date as date, REVERSE(SUBSTRING(REVERSE(\"Item Type skuCode\"),CHARINDEX(\'-\',REVERSE(\"Item Type skuCode\")) + 1))AS sku_group, count(distinct \"Item Code\") available_units, sum(\"Unit price with tax\") as price_with_tax, sum(\"Unit price without tax\") price_without_tax, min(\"Unit price with tax\")Unit_price_with_tax, min(\"Unit price without tax\")Unit_price_without_tax from snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day group by 1,2 )inv left join ( select sku_group.sku_group, sku_group.order_date, sku_group.sold_quantity, sku_group.SALES, sku_group.DISCOUNT, sku_group.mrp_sales, sku_group.Price_Point, sku_level.SOLD_AT_MRP, sku_level.SOLD_AT_DISCOUNT, sku_group.SHIPPING_QUANTITY, sku_group.SUBORDER_QUANTITY from (select order_date::date as order_date, sku_group, SUM(CASE WHEN marketplace_mapped = \'AJIO\' THEN (selling_price * 1.42) ELSE selling_price END) AS SALES, SUM(DISCOUNT) AS DISCOUNT, sum(mrp * coalesce(shipping_quantity,suborder_quantity)::int) as mrp_sales, sum(coalesce(shipping_quantity,suborder_quantity))::int as sold_quantity, SUM(SUBORDER_QUANTITY) AS SUBORDER_QUANTITY, SUM(SHIPPING_QUANTITY) AS SHIPPING_QUANTITY, max(MRP) AS Price_Point from (select uf.*,fi.tags from snitch_db.maplemonk.unicommerce_fact_items_snitch uf left join snitch_db.maplemonk.fact_items_snitch fi on uf.saleorderitemcode = fi.line_item_id where lower(uf.order_status) not like \'%cancel%\' and lower(tags) not like \'%eco%\' ) group by 1,2 )sku_group left join ( select sku_group, order_date, sum(ifnull(SOLD_AT_MRP,0)) as SOLD_AT_MRP, sum(ifnull(SOLD_AT_DISCOUNT,0)) as SOLD_AT_DISCOUNT from ( select sku,sku_group,order_date::date as order_date ,order_name, case when selling_price = mrp*(coalesce(shipping_quantity,shipping_quantity))::int then coalesce(shipping_quantity,suborder_quantity)::int end as SOLD_AT_MRP, case when selling_price != mrp*(coalesce(shipping_quantity,suborder_quantity))::int then coalesce(shipping_quantity,suborder_quantity)::int end as SOLD_AT_DISCOUNT from (select uf.*,fi.tags from snitch_db.maplemonk.unicommerce_fact_items_snitch uf left join snitch_db.maplemonk.fact_items_snitch fi on uf.saleorderitemcode = fi.line_item_id where lower(uf.order_status) not like \'%cancel%\' and lower(tags) not like \'%eco%\' ) ) group by 1,2 )sku_level on sku_level.sku_group = sku_group.sku_group and sku_group.order_date=sku_level.order_date )ufi on inv.date=ufi.order_date and ufi.sku_group = inv.sku_group left join ( select date::date as inverted_date, sku_group, sum(quantity) as units_inverted from snitch_db.maplemonk.sku_class_mapping group by 1,2 order by inverted_date DESC )scm on inv.date = scm.inverted_date and inv.sku_group = scm.sku_group left join ( SELECT * FROM ( select distinct sku_group, CATEGORY, sleeve_type, collar_type, FABRIC, hem, design, closure, fit, occassion, color, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY 1) RN from snitch_db.maplemonk.unicommerce_availability_merge ) WHERE RN=1 ) uam on inv.sku_group = uam.sku_group LEFT JOIN ( SELECT * FROM ( SELECT DISTINCT SKU_GROUP, PRICE, ROW_NUMBER() OVER (PARTITION BY SKU_GROUP ORDER BY 1) RN FROM ( WITH RankedProducts AS ( SELECT REVERSE(SUBSTRING(REVERSE(SKU), CHARINDEX(\'-\', REVERSE(SKU)) + 1)) AS sku_group, PRICE, ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY UPDATED_AT DESC) AS RowNum FROM snitch_db.maplemonk.shopify_all_products_variants ) SELECT sku_group, PRICE FROM RankedProducts WHERE RowNum = 1)) WHERE RN=1 ) ABCD ON inv.sku_group = ABCD.sku_group ) select * , div0( sum(sold_quantity) over(partition by last_day(date),category order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),category order by 1)+ sum(units_inwarded) over(partition by last_day(date),category order by 1 ) ) ) STR_category_mtd, div0(sum(SOLD_AT_MRP) over(partition by last_day(date),category order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),category order by 1)+ sum(units_inwarded) over(partition by last_day(date),category order by 1 ) ) ) STR_category_at_mrp_mtd, div0(sum(SOLD_AT_discount) over(partition by last_day(date),category order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),category order by 1)+ sum(units_inwarded) over(partition by last_day(date),category order by 1 ) ) ) STR_category_at_discount_mtd, div0(sum(units_inwarded) over(partition by last_day(date),category order by 1), sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),category order by 1)) IR_category_mtd, div0(sum(sold_quantity) over(partition by last_day(date),SKU_GROUP order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),SKU_GROUP order by 1)+ sum(units_inwarded) over(partition by last_day(date),SKU_GROUP order by 1 ) ) ) STR_sku_group_mtd, div0(sum(SOLD_AT_MRP) over(partition by last_day(date),SKU_GROUP order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),SKU_GROUP order by 1)+ sum(units_inwarded) over(partition by last_day(date),SKU_GROUP order by 1 ) )) STR_SKU_GROUP_at_mrp_mtd, div0(sum(SOLD_AT_discount) over(partition by last_day(date),SKU_GROUP order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),SKU_GROUP order by 1)+ sum(units_inwarded) over(partition by last_day(date),SKU_GROUP order by 1 ) ) )STR_SKU_GROUP_at_discount_mtd, div0(sum(sold_quantity) over(partition by last_day(date),color order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),color order by 1)+ sum(units_inwarded) over(partition by last_day(date),color order by 1 ) ) ) STR_color_mtd, div0(sum(SOLD_AT_MRP) over(partition by last_day(date),color order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),color order by 1)+ sum(units_inwarded) over(partition by last_day(date),color order by 1 ) ) ) STR_color_at_mrp_mtd, div0(sum(SOLD_AT_discount) over(partition by last_day(date),color order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),color order by 1)+ sum(units_inwarded) over(partition by last_day(date),color order by 1 ) ) ) STR_color_at_discount_mtd, div0(sum(sold_quantity) over(partition by last_day(date),design order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),design order by 1)+ sum(units_inwarded) over(partition by last_day(date),design order by 1 ) ) ) STR_design_mtd, div0(sum(SOLD_AT_MRP) over(partition by last_day(date),design order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),design order by 1)+ sum(units_inwarded) over(partition by last_day(date),design order by 1 ) ) ) STR_design_at_mrp_mtd, div0(sum(SOLD_AT_discount) over(partition by last_day(date),design order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),design order by 1)+ sum(units_inwarded) over(partition by last_day(date),design order by 1 ) ) ) STR_design_at_discount_mtd, div0(sum(sold_quantity) over(partition by last_day(date),fit order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),fit order by 1)+ sum(units_inwarded) over(partition by last_day(date),fit order by 1 ) ) ) STR_fit_mtd, div0(sum(SOLD_AT_MRP) over(partition by last_day(date),fit order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),fit order by 1)+ sum(units_inwarded) over(partition by last_day(date),fit order by 1 ) ) ) STR_fit_at_mrp_mtd, div0(sum(SOLD_AT_discount) over(partition by last_day(date),fit order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),fit order by 1)+ sum(units_inwarded) over(partition by last_day(date),fit order by 1 ) ) ) STR_fit_at_discount_mtd, div0(sum(sold_quantity) over(partition by last_day(date),occassion order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),occassion order by 1)+ sum(units_inwarded) over(partition by last_day(date),occassion order by 1 ) ) ) STR_occassion_mtd, div0(sum(SOLD_AT_MRP) over(partition by last_day(date),occassion order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),occassion order by 1)+ sum(units_inwarded) over(partition by last_day(date),occassion order by 1 ) ) ) STR_occassion_at_mrp_mtd, div0(sum(SOLD_AT_discount) over(partition by last_day(date),occassion order by 1), ( sum(case when date=DATE_TRUNC(\'month\', date) then (available_units) end ) over(partition by last_day(date),occassion order by 1)+ sum(units_inwarded) over(partition by last_day(date),occassion order by 1 ) ) ) STR_occassion_at_discount_mtd from Day_level_data ORDER BY DATE DESC;",
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
                        