{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.sellable_inv_across_facilities AS SELECT DATE::date AS date, gs.cluster as STATE, CASE WHEN UPPER(STATE) in (\'DADRA AND NAGAR HAVELI\',\'GUJARAT\',\'MADHYA PRADESH\',\'CHHATISGARH\',\'GOA\',\'MAHARASHTRA\') THEN \'WEST\' WHEN UPPER(STATE) in (\'ANDHRA PRADESH\',\'KARNATAKA\',\'PONDICHERRY\',\'TAMIL NADU\',\'TELANGANA\',\'KERALA\') THEN \'SOUTH\' WHEN UPPER(STATE) in (\'CHANDIGARH\',\'DELHI\',\'HARYANA\',\'HIMACHAL PRADESH\',\'JAMMU AND KASHMIR\',\'PUNJAB\',\'RAJASTHAN\',\'UTTAR PRADESH\',\'UTTARAKHAND\') THEN \'NORTH\' WHEN UPPER(STATE) in (\'ARUNACHAL PRADESH\',\'ASSAM\',\'BIHAR\',\'JHARKHAND\',\'MEGHALAYA\',\'MIZORAM\',\'NAGALAND\',\'ODISHA\',\'SIKKIM\',\'TRIPURA\',\'WEST BENGAL\') THEN \'EAST\' ELSE \'UNMAPPED\' END AS ZONE, gs.region as CITY, SUBSTRING(BRANCH_NAME,10,4) as TYPE, SUBSTRING(BRANCH_NAME,16,25) as branch_name, LOGICUSERCODE AS SKU, CASE WHEN POSITION(\'-\' IN LOGICUSERCODE) > 0 AND LEFT(LOGICUSERCODE, 2) = \'SH\' AND LENGTH(LOGICUSERCODE) < 10 THEN UPPER(SPLIT_PART(LOGICUSERCODE, \'-\', 1)) WHEN POSITION(\'-\' IN LOGICUSERCODE) > 0 THEN UPPER(SPLIT_PART(LOGICUSERCODE, \'-\', 1)) || \'-\' || UPPER(SPLIT_PART(LOGICUSERCODE, \'-\', 2)) ELSE UPPER(LOGICUSERCODE) END AS sku_group, SUM(STOCK_QTY) AS inventory FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand fsd LEFT JOIN SNITCH_DB.MAPLEMONK.OFFLINE_STORE_DETAILED_MAPPING gs ON fsd.branch_name = gs.store_name WHERE DATE::date = CURRENT_DATE() AND godown_name IN (\'POS\', \'FRANCHISE\') GROUP BY 1, 2, 3, 4,5,6,7,8",
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
            