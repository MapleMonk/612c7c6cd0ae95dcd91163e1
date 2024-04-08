{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_Shopify_logistics_cost as with pickup_detail as ( select distinct REFERENCE_CODE, trim(pickup_pin_code) as pickup_pin_code from HOX_DB.MAPLEMONK.EasyEcom_EasyEcom_BLANKO_HOX_CUSTOMER_ORDERS ), shipping_zone as ( select lower(SHIP_CITY) as SHIP_CITY, lower(SHIP_STATE) as SHIP_STATE, trim(try_cast(SHIP_PINCODE as float)) as SHIP_PINCODE, lower(XB_BILLING_BLR_ZONE) as XB_BILLING_BLR_ZONE, lower(\"XB_Billing _Delhi_Zone\") as XB_Billing_Delhi_Zone, lower(DELHIVERY_BILLING_BLR_ZONE) as DELHIVERY_BILLING_BLR_ZONE, lower(\"Delhivery_Billing _Delhi_Zone\") as Delhivery_Billing_Delhi_Zone from HOX_DB.MAPLEMONK.SHIPPING_ZONE ), easyecom_data as ( select distinct o.*, case when lower(Company_Name) = \'blanko ggn\' and courier = \'xpressbees\' then XB_BILLING_DELHI_ZONE when lower(Company_Name) = \'blanko ggn\' and courier = \'delhivery\' then DELHIVERY_BILLING_DELHI_ZONE when lower(Company_Name) = \'blanko blr\' and courier = \'xpressbees\' then XB_BILLING_BLR_ZONE when lower(Company_Name) = \'blanko blr\' and courier = \'delhivery\' then DELHIVERY_BILLING_BLR_ZONE end as zone from ( select distinct \'Easyecom sale\' as report_type, case when trim(UPPER(\"Company Name\")) in (\'BLANKO GGN\', \'MOJOJOJO CREATORS PVT LTD GGN\') then \'BLANKO GGN\' when trim(upper(\"Company Name\")) in (\'BLANKO BLR\', \'MOJOJOJO CREATORS PVT LTD BLR\') then \'BLANKO BLR\' else \"Company Name\" end as Company_Name, case when \"Reference Code\" = \'HOX_BLANKO_0070_PR\' then \'Shopify\' else \"MP Name\" end as marketplace, \"Reference Code\" as Reference_Code, case when \"Reference Code\" = \'HOX_BLANKO_0070_PR\' then \'B2C\' else \"Order Type\" end as order_type, lower(COURIER) as courier, try_cast(\"Zip Code\" as float) as drop_pincode, case when lower(\"Payment Mode\") = \'online\' then \'prepaid\' else lower(\"Payment Mode\") end as payment_mode, trim(pickup_pin_code)as pickup_pin_code, sum(\"Order Invoice Amount\")as total_invoice from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_sales a left join pickup_detail b on a.\"Reference Code\" = b.REFERENCE_CODE WHERE a.\"Reference Code\" not like \'%_DEL_%\' group by 1,2,3,4,5,6,7,8,9 )as o left join shipping_zone a on o.drop_pincode = a.SHIP_PINCODE ), shopify_shipping as ( select date(try_cast(START_DATE as timestamp)) as START_DATE, date(try_cast(END_DATE as timestamp)) as END_DATE, COURIER_TYPE, lower(PAYMENT_MODE) as PAYMENT_MODE, \"region / tier\" as region, SHIPMENT_TYPE, lower(COURIER_PARTNER) as COURIER_PARTNER, try_cast(START_WEIGHT as float) as START_WEIGHT, try_cast(END_WEIGHT as float) as END_WEIGHT, try_cast(\"FSC \" as float) as FSC, try_cast(return_FSC as float) as return_FSC, try_cast(RTO_BASE_PRICE as float) as RTO_BASE_PRICE, try_cast(PAYMENT_MODE_PERC as float) as PAYMENT_MODE_PERC, try_cast(FORWARD_BASE_PRICE as float) as FORWARD_BASE_PRICE, try_cast(INCREMENTAL_WEIGHT as float) as INCREMENTAL_WEIGHT, try_cast(PAYMENT_MODE_CHARGE as float) as PAYMENT_MODE_CHARGE, try_cast(RTO_INCREMENTAL_PRICE as float) as RTO_INCREMENTAL_PRICE, try_cast(FORWARD_INCREMENTAL_PRICE as float) as FORWARD_INCREMENTAL_PRICE from HOX_DB.MAPLEMONK.SHOPIFY_SHIPPING_CHARGES ), returns as ( select distinct \"Reference Code\" as Reference_Code from HOX_DB.MAPLEMONK.easyecom_hox_blanko_tax_returns WHERE \"Reference Code\" not like \'%_DEL_%\' and \"Reference Code\" <> \'Amazon_FBA_Blanko_13\' ), base as ( select distinct o.*, a.Company_Name, a.COURIER, a.drop_pincode, a.payment_mode, a.pickup_pin_code, a.total_invoice, a.zone, case when lower(a.zone) = \'local\' then \'surface\' else \'air\' end as zone_type, case when c.reference_code is null then \'not return\' else \'return\' end as return_status from ( select distinct reference_code, report_date, marketplace, report_type, FINAL_ORDER_WEIGHT from hox_db.maplemonk.HOX_BLANKO_packaging_material where lower(marketplace) in (\'shopify\', \'retail customers\') )as o left join easyecom_data a on o.reference_code = a.reference_code left join returns c on o.reference_code = c.reference_code ) select distinct o.*, case when round(COD_FIX_charge,2) > round(COD_perc_charge,2) then round((COD_FIX_charge + Prepaid_charge),2) else round((COD_perc_charge + Prepaid_charge),2) end as final_shipping from ( select a.*, Case when lower(a.payment_mode) = \'prepaid\' and FINAL_ORDER_WEIGHT <= 0.5 then CASE WHEN return_status=\'return\' THEN (FORWARD_BASE_PRICE + ((FSC/100)*FORWARD_BASE_PRICE)) + (RTO_BASE_PRICE + ((return_FSC/100)*RTO_BASE_PRICE)) ELSE FORWARD_BASE_PRICE + ((FSC/100)*FORWARD_BASE_PRICE) end when lower(a.payment_mode) = \'prepaid\' and FINAL_ORDER_WEIGHT > 0.5 then CASE WHEN return_status = \'return\' THEN ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE)) + ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE))* (FSC/100))) + ((RTO_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*RTO_INCREMENTAL_PRICE)) + ((RTO_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*RTO_INCREMENTAL_PRICE))* (return_FSC/100))) ELSE ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE)) + ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE))* (FSC/100))) end ELSE 0 end as Prepaid_charge, Case when lower(a.payment_mode) = \'cod\' and FINAL_ORDER_WEIGHT <= 0.5 then CASE WHEN return_status=\'return\' THEN (FORWARD_BASE_PRICE + ((FSC/100)*FORWARD_BASE_PRICE)) + (RTO_BASE_PRICE + ((return_FSC/100)*RTO_BASE_PRICE)) ELSE FORWARD_BASE_PRICE + ((FSC/100)*FORWARD_BASE_PRICE) + PAYMENT_MODE_CHARGE end when lower(a.payment_mode) = \'cod\' and FINAL_ORDER_WEIGHT > 0.5 then CASE WHEN return_status = \'return\' THEN ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE)) + ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE))* (FSC/100))) + ((RTO_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*RTO_INCREMENTAL_PRICE)) + ((RTO_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*RTO_INCREMENTAL_PRICE))* (return_FSC/100))) ELSE ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE)) + ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE))* (FSC/100))) + PAYMENT_MODE_CHARGE end ELSE 0 end as COD_FIX_charge, Case when lower(a.payment_mode) = \'cod\' and FINAL_ORDER_WEIGHT <= 0.5 then CASE WHEN return_status=\'return\' THEN (FORWARD_BASE_PRICE + ((FSC/100)*FORWARD_BASE_PRICE)) + (RTO_BASE_PRICE + ((return_FSC/100)*RTO_BASE_PRICE)) ELSE FORWARD_BASE_PRICE + ((FSC/100)*FORWARD_BASE_PRICE) + ((payment_mode_perc/100)*total_invoice) end when lower(a.payment_mode) = \'cod\' and FINAL_ORDER_WEIGHT > 0.5 then CASE WHEN return_status = \'return\' THEN ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE)) + ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE))* (FSC/100))) + ((RTO_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*RTO_INCREMENTAL_PRICE)) + ((RTO_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*RTO_INCREMENTAL_PRICE))* (return_FSC/100))) ELSE ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE)) + ((FORWARD_BASE_PRICE + (ceil((FINAL_ORDER_WEIGHT - 0.5)/0.5)*FORWARD_INCREMENTAL_PRICE))* (FSC/100))) + ((payment_mode_perc/100)*total_invoice) end ELSE 0 end as COD_perc_charge FROM base a left join shopify_shipping b on (a.report_date between b.START_DATE and b.END_DATE) and lower(a.payment_mode) = lower(b.PAYMENT_MODE) and lower(a.COURIER) = lower(b.COURIER_PARTNER) and lower(a.zone_type) = lower(b.Courier_type) and lower(a.zone) = lower(b.region) )as o",
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
                        