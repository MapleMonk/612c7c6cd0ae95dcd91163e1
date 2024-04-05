{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_amazon_marketplace_fees as with abc as ( select *, (SHIPPING_AMOUNT_BASIS + GIFT_WRAP_AMOUNT_BASIS + SHIPPING_PROMO_DISCOUNT_BASIS) as chargeback, ((principal_amount + item_promo_discount)/QUANTITY) as item_value from HOX_DB.MapleMonk.HOX_BLANKO_Amazon_Tax_Fact_Items where lower(report_type) = \'amazon sales\' and lower(ORDER_STATUS) not like \'%cancel%\' ), base as ( select order_id, case when upper(shipment_level) in (\'SECONDDAY\', \'NEXTDAY\') then \'prime\' else lower(shipment_level) end as shipment_level, case when lower(source) = \'amazon\' then \'Amazon FBA\' else \'Amazon EasyShip\' end as marketplace from HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items group by 1,2, 3 ), base_data as ( select abc.*, base.marketplace, base.shipment_level from abc inner join base on abc.reference_code = base.order_id ), base_fees as ( select date(try_cast(\"start date\" as timestamp)) as \"start date\", date(try_cast(\"end date\" as timestamp)) as \"end date\", \"charge details\", \"type of charge\", \"type of value\", \"order type\", Fullfilment, try_cast(\"start bucket\" as float) as \"start bucket\", try_cast(\"end bucket\" as float) as \"end bucket\", try_cast(charges as float) as charges, try_cast(\"All category\" as float) as \"All category\", try_cast(\"select category\" as float) as \"select category\", try_cast(\"eashyship prime charges\" as float) as \"eashyship prime charges\", try_cast(\"final charge\" as float) as \"final charge\" from hox_db.maplemonk.marketplace_fees_amazon_commision ), base_data_charges as ( select a.*, date(date_trunc(\'month\', REPORT_DATE))as month_report_date, round(((a.item_value * b.\"final charge\" / 100.00)* a.quantity),2) as Commission_value, case when lower(a.shipment_level) = \'prime\' and lower(a.marketplace) = \'amazon easyship\' then round((c.\"eashyship prime charges\" * a.quantity),2) else round((c.\"final charge\" * a.quantity),2) end as Closing_fees, round((d.\"final charge\" * a.quantity),2) as FBA_pick_pack from base_data a left join base_fees b on (date(a.report_date) between date(b.\"start date\") and date(b.\"end date\")) and lower(b.\"charge details\") = \'commission\' and (a.item_value between b.\"start bucket\" and b.\"end bucket\") left join base_fees c on (date(a.report_date) between date(c.\"start date\") and date(c.\"end date\")) and lower(c.\"charge details\") = \'closing fees\' and (a.item_value between c.\"start bucket\" and c.\"end bucket\") and lower(a.marketplace) = lower(c.Fullfilment) left join base_fees d on (date(a.report_date) between date(d.\"start date\") and date(d.\"end date\")) and lower(d.\"charge details\") = \'pick and pack fees (packaging)\' and lower(a.marketplace) = lower(d.Fullfilment) ), settlement_base_data as ( select date(try_cast(date as timestamp)) as date, type, \"order id\" as reference_code, description, case when lower(fulfillment) = \'amazon\' then \'Amazon FBA\' when lower(fulfillment) = \'merchant\' then \'Amazon EasyShip\' else null end as fulfillment, try_cast(replace(quantity,\',\',\'\') as float) as quantity, try_cast(replace(\"selling fees\",\',\',\'\') as float) as Selling_fees, try_cast(replace(\"fba fees\",\',\',\'\') as float) as FBA_fees, try_cast(replace(\"other transaction fees\",\',\',\'\') as float) as other_transaction_fees, try_cast(replace(other,\',\',\'\') as float) as other, try_cast(replace(total,\',\',\'\') as float) as total from HOX_DB.MapleMonk.AMAZON_SHIPPING_FEES_SETTLEMENT ), final_shipping_charges as ( select o.reference_code, o.marketplace, abs(coalesce(round(sum(a.shipping_fees),2),0)) as final_shipping from ( select distinct reference_code, marketplace from base_data_charges where lower(marketplace) = \'amazon easyship\' )o inner join ( select reference_code, coalesce(sum(other_transaction_fees/1.18),0) as shipping_fees from settlement_base_data where lower(type) = \'shipping services\' group by 1 )a on o.reference_code = a.reference_code group by 1, 2 union select o.reference_code, o.marketplace, abs(round(sum(a.FBA_fees + o.FBA_pick_pack + o.chargeback),2)) as final_shipping from ( select reference_code, marketplace, coalesce(sum(FBA_pick_pack),0) as FBA_pick_pack, coalesce(sum(chargeback),0) as chargeback from base_data_charges where lower(marketplace) = \'amazon fba\' group by 1, 2 )o inner join ( select reference_code, coalesce(sum(FBA_fees/1.18),0) as FBA_fees from settlement_base_data where lower(type) = \'order\' group by 1 )a on o.reference_code = a.reference_code group by 1, 2 ), final_storage_fees as ( select date(date_trunc(\'month\', date))as month_date, round(coalesce(sum(other/1.18),0),2) as storage_cost from settlement_base_data where lower(type) = \'fba inventory fee\' and lower(description) = \'fba inventory storage fee\' group by 1 order by 1 ), amazon_fba_removal as ( select date(date_trunc(\'month\', date))as month_date, round(coalesce(sum(other/1.18),0),2) as fba_removal_fees from settlement_base_data where lower(description) = \'fba removal order: return fee\' group by 1 order by 1 ), amazon_fba_storage_removal as ( select reference_code, round(div0(storage_cost, count(1) over (partition by o.month_report_date order by 1)),2)as storage_cost, round(div0(fba_removal_fees, count(1) over (partition by o.month_report_date order by 1)),2)as fba_removal_fees from ( select distinct reference_code, month_report_date from base_data_charges where lower(marketplace) = \'amazon fba\' )as o left join final_storage_fees a on o.month_report_date = a.month_date left join amazon_fba_removal b on o.month_report_date = b.month_date ) select distinct a.* , round(div0(b.final_shipping, count(1) over (partition by a.Reference_Code order by 1)),2) as final_shipping, abs(round(div0(c.storage_cost, count(1) over (partition by a.Reference_Code order by 1)),2)) as storage_cost, abs(round(div0(c.fba_removal_fees, count(1) over (partition by a.Reference_Code order by 1)),2)) as fba_removal_fees from base_data_charges a left join final_shipping_charges b on a.reference_code = b.reference_code left join amazon_fba_storage_removal c on a.reference_code = c.reference_code",
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
                        