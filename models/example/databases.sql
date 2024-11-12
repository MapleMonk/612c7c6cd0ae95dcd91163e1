{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table MAPLEMONK.KA_AMAZON_VENDOR_CENTRAL_PO_FACT_ITEMS as select \'AMAZON VENDOR CENTRAL PO\' marketplace ,ASIN PRODUCT_ID ,AM.COMMONSKUCODE COMMONSKU ,p.name PRODUCT_NAME_FINAL ,p.category ,p.sub_category ,p.tax_rate ,upper(Title) PRODUCT_NAME ,Status ,cast(replace(Discount,\'%\',\'\') as float64)/100 Discount ,Priority ,cast(Case_Size as int64) Case_Size ,cast(Cost_price as float64) cost_price ,cast(List_Price as float64) List_Price ,Order_PO_Number ,parse_date(\'%d-%b-%Y\',Order_Date) Order_Date ,External_Id_Type ,External_ID ,Vendor_Code ,Merchant_Sku ,Model_Number ,Currency_Code ,upper(Freight_Terms) Freight_Terms ,upper(Hand_Off_Type) Hand_Off_Type ,parse_date(\'%d-%b-%Y\',Hand_Off_Start) Hand_Off_Start ,parse_date(\'%d-%b-%Y\',Hand_Off_End) Hand_Off_End ,parse_date(\'%d-%b-%Y\',Expected_Hand_Off_Date) Expected_Hand_Off_Date ,Consolidation_ID ,Fulfillment_Center ,Availability_Status ,safe_cast(Total_requested_cost as float64) Total_requested_cost ,safe_cast(Total_accepted_cost as float64) Total_accepted_cost ,safe_cast(Total_received_cost as float64) Total_received_cost ,safe_cast(Total_cancelled_cost as float64) Total_cancelled_cost ,safe_Cast(ASN_quantity__units_ as int64) ASN_Quantity_Units ,safe_Cast(Requested_quantity__cases_ as int64) Requested_quantity_cases ,safe_Cast(Requested_quantity__units_ as int64) Requested_quantity_units ,safe_Cast(Accepted_quantity__cases_ as int64) Accepted_quantity_cases ,safe_Cast(Accepted_quantity__units_ as int64) Accepted_quantity_units ,safe_Cast(Received_quantity__cases_ as int64) Received_quantity_cases ,safe_Cast(Received_quantity__units_ as int64) Received_quantity_units ,safe_Cast(Cancelled_quantity__cases_ as int64) Cancelled_quantity_cases ,safe_Cast(Cancelled_quantity__units_ as int64) Cancelled_quantity_units ,safe_Cast(Remaining_quantity__cases_ as int64) Remaining_quantity_cases ,safe_Cast(Remaining_quantity__units_ as int64) Remaining_quantity_units from `MapleMonk.KA_S3_Amazon_PO` fi left join (select * from `MapleMonk.KA_GS_SKU_MarketplaceSKU_Mapping` qualify row_number()over (partition by AMAZON_ASIN order by 1) = 1 ) AM on upper(fi.ASIN) = upper(AM.AMAZON_ASIN) left join (select commonsku skucode, name, category, category_code sub_category, category_code, commonsku, TAX_RATE from maplemonk.final_sku_master qualify row_number()over (partition by commonsku order by 1) = 1 ) p on lower(AM.COMMONSKUCODE) = lower(p.skucode) ;",
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
            