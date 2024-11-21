{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.KA_Tally_B2B_SALES_FACT_ITEMS AS select PARSE_DATE(\'%d-%b-%y\', date) Order_Date ,Order_Number REFERENCE_CODE ,Voucher_Number Order_ID ,upper(Party_Name) Name ,UPPER(coalesce(MAP.Mapped_B2B_name, TD.VOUCHER_TYPE)) MARKETPLACE ,Item_Part_No SKUCODE ,p.commonsku ,upper(coalesce(p.name, TD.ITEM_NAME)) Product_Name_Final ,upper(coalesce(p.category, TD.Item_Group)) Category ,upper(coalesce(p.sub_Category, TD.Item_Category)) Sub_Category ,coalesce(safe_cast(TD.Item_rate_of_GST as float64)/100, p.Tax_rate) Tax_Rate ,Party_Pincode Pincode ,Tracking_Number ,Party_Mobile_No_ Phone ,Party_E_Mail Email ,upper(Company_Name) warehouse ,upper(Party_State) state ,sum(safe_Cast(TD.Billed_Quantity as float64)) Quantity ,sum(safe_Cast(TD.Discount_Amount as float64)) Discount ,sum(safe_Cast(TD.Amount as float64)*(1+coalesce(safe_cast(TD.Item_rate_of_GST as float64)/100, p.Tax_rate))) Selling_Price from `MapleMonk.KA_S3_B2B_Daily_Sales_detailed` TD left join (select marketplace, CUSTOMER_NAME, upper(Mapped_B2B_Name) Mapped_B2B_name, upper(Include_Sales) Include_Sales from maplemonk.ka_gs_B2B_Customer_Mapping qualify row_number() over (partition by lower(marketplace), lower(customer_name) order by 1) =1 ) MAP on upper(TD.VOUCHER_TYPE) = upper(MAP.marketplace) and upper(TD.party_name) = upper(MAP.customer_name) left join (select commonsku skucode, name, category, category_code sub_category, category_code, commonsku, TAX_RATE from maplemonk.final_sku_master qualify row_number()over (partition by commonsku order by 1) = 1 ) p on lower(replace(TD.Item_Part_No,\' \',\'\')) = lower(p.skucode) where MAP.Include_Sales = \'YES\' group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17 ;",
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
            