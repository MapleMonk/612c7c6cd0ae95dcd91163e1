{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.size_ratio_production_eoq as with first_check as ( select sku_group, repeat, size_tag, revised_order_quant, category, case when size_tag = \'with_xs_with_xxl\' then LEAST(NULLIF(xs_order_quant, 0),NULLIF(s_order_quant, 0),NULLIF(m_order_quant, 0),NULLIF(l_order_quant, 0),NULLIF(xl_order_quant, 0),NULLIF(xxl_order_quant, 0)) when size_tag = \'without_xs_without_xxl\' then LEAST(NULLIF(s_order_quant, 0),NULLIF(m_order_quant, 0),NULLIF(l_order_quant, 0),NULLIF(xl_order_quant, 0)) when size_tag = \'with_xs_without_xxl\' then LEAST(NULLIF(xs_order_quant, 0),NULLIF(s_order_quant, 0),NULLIF(m_order_quant, 0),NULLIF(l_order_quant, 0),NULLIF(xl_order_quant, 0)) when size_tag = \'without_xs_with_xxl\' then LEAST(NULLIF(s_order_quant, 0),NULLIF(m_order_quant, 0),NULLIF(l_order_quant, 0),NULLIF(xl_order_quant, 0),NULLIF(xxl_order_quant, 0)) end as least_quant from snitch_db.maplemonk.final_output_eoq ), main as ( select a.sku_group, a.repeat, a.size_tag, a.revised_order_quant, a.category, xs_order_quant/least_quant as xs_ratio , s_order_quant/least_quant as s_ratio, l_order_quant/least_quant as l_ratio, m_order_quant/least_quant as m_ratio, xl_order_quant/least_quant as xl_ratio, xxl_order_quant/least_quant as xxl_ratio from snitch_db.maplemonk.final_output_eoq a left join first_check b on a.sku_group = b.sku_group where a.repeat = \'yes\' ) select sku_group, concat(round(xs_ratio,1),\' : \',round(s_ratio,1),\' : \',round(m_ratio,1),\' : \',round(l_ratio,1),\' : \',round(xl_ratio,1),\' : \',round(xxl_ratio,1)) as size_ratio from main",
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
            