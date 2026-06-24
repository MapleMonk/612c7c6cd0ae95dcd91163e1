{{ config(
            materialized='table',
                post_hook={
                    "sql": "TRUNCATE TABLE `kerala-ayurveda-wh.MapleMonk.kal_zepto_po_line_level_fact_table`; INSERT INTO `kerala-ayurveda-wh.MapleMonk.kal_zepto_po_line_level_fact_table` SELECT `_airbyte_unique_key`, NULLIF(TRIM(`EAN`), \'\') AS `EAN`, NULLIF(TRIM(`HSN`), \'\') AS `HSN`, SAFE_CAST(REPLACE(NULLIF(TRIM(`MRP`), \'\'), \',\', \'\') AS FLOAT64) AS `MRP`, SAFE_CAST(REPLACE(NULLIF(TRIM(`Qty`), \'\'), \',\', \'\') AS FLOAT64) AS `Qty`, NULLIF(TRIM(`SKU`), \'\') AS `SKU`, NULLIF(TRIM(`Brand`), \'\') AS `Brand`, SAFE_CAST(REPLACE(NULLIF(TRIM(`CESS__`), \'\'), \',\', \'\') AS FLOAT64) AS `CESS__`, SAFE_CAST(REPLACE(NULLIF(TRIM(`CGST__`), \'\'), \',\', \'\') AS FLOAT64) AS `CGST__`, SAFE_CAST(REPLACE(NULLIF(TRIM(`IGST__`), \'\'), \',\', \'\') AS FLOAT64) AS `IGST__`, NULLIF(TRIM(`PO_No_`), \'\') AS `PO_No_`, SAFE_CAST(REPLACE(NULLIF(TRIM(`SGST__`), \'\'), \',\', \'\') AS FLOAT64) AS `SGST__`, NULLIF(TRIM(`Status`), \'\') AS `Status`, NULLIF(TRIM(`Line_No`), \'\') AS `Line_No`, SAFE.PARSE_DATETIME(\'%d %b %Y %I:%M %p\', NULLIF(TRIM(`PO_Date`), \'\')) AS `PO_Date`, NULLIF(TRIM(`SKU_Code`), \'\') AS `SKU_Code`, NULLIF(TRIM(`SKU_Desc`), \'\') AS `SKU_Desc`, SAFE_CAST(REPLACE(NULLIF(TRIM(`PO_Amount`), \'\'), \',\', \'\') AS FLOAT64) AS `PO_Amount`, NULLIF(TRIM(`Created_By`), \'\') AS `Created_By`, NULLIF(TRIM(`Vendor_Code`), \'\') AS `Vendor_Code`, NULLIF(TRIM(`Vendor_Name`), \'\') AS `Vendor_Name`, SAFE_CAST(REPLACE(NULLIF(TRIM(`ASN_Quantity`), \'\'), \',\', \'\') AS FLOAT64) AS `ASN_Quantity`, NULLIF(TRIM(`Del_Location`), \'\') AS `Del_Location`, SAFE_CAST(REPLACE(NULLIF(TRIM(`GRN_Quantity`), \'\'), \',\', \'\') AS FLOAT64) AS `GRN_Quantity`, SAFE_CAST(REPLACE(NULLIF(TRIM(`Landing_Cost`), \'\'), \',\', \'\') AS FLOAT64) AS `Landing_Cost`, SAFE_CAST(REPLACE(NULLIF(TRIM(`Total_Amount`), \'\'), \',\', \'\') AS FLOAT64) AS `Total_Amount`, SAFE.PARSE_DATETIME(\'%d %b %Y %I:%M %p\', NULLIF(TRIM(`PO_Expiry_Date`), \'\')) AS `PO_Expiry_Date`, SAFE_CAST(REPLACE(NULLIF(TRIM(`Unit_Base_Cost`), \'\'), \',\', \'\') AS FLOAT64) AS `Unit_Base_Cost`, SAFE_CAST(NULLIF(TRIM(`POLineLevel_Date`), \'\') AS DATE) AS `POLineLevel_Date`, `_airbyte_ab_id`, `_airbyte_emitted_at`, `_airbyte_normalized_at`, `_airbyte_kal_zepto_po_line_level_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM `kerala-ayurveda-wh.MapleMonk.kal_zepto_po_line_level`;",
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
            