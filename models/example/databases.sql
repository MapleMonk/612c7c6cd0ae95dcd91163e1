{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.KA_Tally_B2B_SALES_FACT_ITEMS AS select PARSE_DATE(\'%d-%b-%y\', date) Order_Date ,Voucher_Number REFERENCE_CODE ,Voucher_Number Order_ID ,upper(Party_Name) Name ,UPPER(coalesce(MAP.Mapped_B2B_name, TD.VOUCHER_TYPE)) MARKETPLACE ,Item_Part_No SKUCODE ,p.skucode commonsku ,upper(coalesce(p.name, TD.ITEM_NAME)) Product_Name_Final ,upper(coalesce(p.category, TD.Item_Group)) Category ,upper(p.sub_Category) Sub_Category ,coalesce(safe_divide(safe_cast(TD.MRP as float64)*(1-safe_cast(TD.Discount as float64)/100),safe_cast(TD.Rate as float64))-1 , p.Tax_rate) Tax_Rate ,cast(null as string) Pincode ,cast(null as string) Tracking_Number ,cast(null as string) Phone ,cast(null as string) Email ,upper(Company_Name) warehouse ,cast(null as string) state ,sum(safe_Cast(TD.Qty as float64)) Quantity ,sum(safe_cast(TD.MRP as float64)*(safe_cast(TD.Discount as float64)/100)*safe_Cast(TD.Qty as float64)) Discount ,sum(safe_Cast(TD.Amount as float64)*(1+coalesce(safe_divide(safe_cast(TD.MRP as float64)*(1-safe_cast(TD.Discount as float64)/100),safe_cast(TD.Rate as float64))-1 , p.Tax_rate))) Selling_Price from `MapleMonk.KA_S3_Trimmed_B2B_Daily_Sales_detailed` TD left join (select marketplace, CUSTOMER_NAME, upper(Mapped_B2B_Name) Mapped_B2B_name, upper(Include_Sales) Include_Sales from maplemonk.ka_gs_Tally_b2b_mapping qualify row_number() over (partition by lower(marketplace), lower(customer_name) order by 1) =1 ) MAP on upper(TD.VOUCHER_TYPE) = upper(MAP.marketplace) and upper(TD.party_name) = upper(MAP.customer_name) left join (select commonsku skucode, name, category, category_code sub_category, category_code, MRP, TAX_RATE from (select * from maplemonk.final_sku_master where lower(marketplace) like \'%unicommerce%\') qualify row_number() over (partition by lower(ifnull(trim(COMMONSKU),\'\')) order by lower(ifnull(trim(COMMONSKU),\'\')) desc) = 1 ) p on lower(replace(TD.Item_Part_No,\' \',\'\')) = lower(replace(p.skucode,\' \',\'\')) where MAP.Include_Sales = \'YES\' group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17 ;",
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
            