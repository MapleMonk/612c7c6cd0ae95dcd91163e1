{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_amazon_marketplace_fees as with abc as ( select *, ((principal_amount + item_promo_discount)/QUANTITY) as item_value from HOX_DB.MapleMonk.HOX_BLANKO_Amazon_Tax_Fact_Items where lower(report_type) = \'amazon sales\' and lower(ORDER_STATUS) not like \'%cancel%\' ), base as ( select order_id, case when upper(shipment_level) in (\'SECONDDAY\', \'NEXTDAY\') then \'prime\' else lower(shipment_level) end as shipment_level, case when lower(source) = \'amazon\' then \'Amazon FBA\' else \'Amazon EasyShip\' end as marketplace from HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items group by 1,2, 3 ), base_data as ( select abc.*, base.marketplace, base.shipment_level from abc inner join base on abc.reference_code = base.order_id ), base_fees as ( select date(try_cast(\"start date\" as timestamp)) as \"start date\", date(try_cast(\"end date\" as timestamp)) as \"end date\", \"charge details\", \"type of charge\", \"type of value\", \"order type\", Fullfilment, try_cast(\"start bucket\" as float) as \"start bucket\", try_cast(\"end bucket\" as float) as \"end bucket\", try_cast(charges as float) as charges, try_cast(\"All category\" as float) as \"All category\", try_cast(\"select category\" as float) as \"select category\", try_cast(\"eashyship prime charges\" as float) as \"eashyship prime charges\", try_cast(\"final charge\" as float) as \"final charge\" from hox_db.maplemonk.marketplace_fees_amazon_commision ) select a.*, round(((a.item_value * b.\"final charge\" / 100.00)* a.quantity),2) as Commission_value, case when lower(a.shipment_level) = \'prime\' and lower(a.marketplace) = \'amazon easyship\' then round((c.\"eashyship prime charges\" * a.quantity),2) else round((c.\"final charge\" * a.quantity),2) end as Closing_fees, round((d.\"final charge\" * a.quantity),2) as \"Pick and Pack fees\" from base_data a left join base_fees b on (date(a.report_date) between date(b.\"start date\") and date(b.\"end date\")) and lower(b.\"charge details\") = \'commission\' and (a.item_value between b.\"start bucket\" and b.\"end bucket\") left join base_fees c on (date(a.report_date) between date(c.\"start date\") and date(c.\"end date\")) and lower(c.\"charge details\") = \'closing fees\' and (a.item_value between c.\"start bucket\" and c.\"end bucket\") and lower(a.marketplace) = lower(c.Fullfilment) left join base_fees d on (date(a.report_date) between date(d.\"start date\") and date(d.\"end date\")) and lower(d.\"charge details\") = \'pick and pack fees (packaging)\' and lower(a.marketplace) = lower(d.Fullfilment)",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        