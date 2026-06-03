{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.quickshift_easyecom_putaway_report; CREATE TABLE public.quickshift_easyecom_putaway_report AS SELECT REPLACE(sku, \'`\', \'\') AS SKU, REPLACE(itemid, \'`\', \'\') AS Item_ID, PONumber, UserName, GrnNumber, UPPER(SerialNum) AS Serial_Number, CurrentBin, OrderNumber, PutAwayType, SubOrderNum, CurrentStatus, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(OriginalPutawayTime AS TIMESTAMP) END AS Original_Putaway_Datetime, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(CAST(OriginalPutawayTime AS TIMESTAMP) AS DATE) END AS Original_Putaway_Date, OriginalBinWhenPutawayDone, \'BENNYS BOWL\' as data_source FROM public.quick_shift_bennys_bowl_putaway_report union ALL SELECT REPLACE(sku, \'`\', \'\') AS SKU, REPLACE(itemid, \'`\', \'\') AS Item_ID, PONumber, UserName, GrnNumber, UPPER(SerialNum) AS Serial_Number, CurrentBin, OrderNumber, PutAwayType, SubOrderNum, CurrentStatus, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(OriginalPutawayTime AS TIMESTAMP) END AS Original_Putaway_Datetime, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(CAST(OriginalPutawayTime AS TIMESTAMP) AS DATE) END AS Original_Putaway_Date, OriginalBinWhenPutawayDone, \'GHAR SOAPS\' as data_source FROM public.quick_shift_ghar_soaps_putaway_report union ALL SELECT REPLACE(sku, \'`\', \'\') AS SKU, REPLACE(itemid, \'`\', \'\') AS Item_ID, PONumber, UserName, GrnNumber, UPPER(SerialNum) AS Serial_Number, CurrentBin, OrderNumber, PutAwayType, SubOrderNum, CurrentStatus, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(OriginalPutawayTime AS TIMESTAMP) END AS Original_Putaway_Datetime, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(CAST(OriginalPutawayTime AS TIMESTAMP) AS DATE) END AS Original_Putaway_Date, OriginalBinWhenPutawayDone, \'QUICKSHIFT EASYECOM\' as data_source FROM public.qs_easyecom_putaway_report union ALL SELECT REPLACE(sku, \'`\', \'\') AS SKU, REPLACE(itemid, \'`\', \'\') AS Item_ID, PONumber, UserName, GrnNumber, UPPER(SerialNum) AS Serial_Number, CurrentBin, OrderNumber, PutAwayType, SubOrderNum, CurrentStatus, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(OriginalPutawayTime AS TIMESTAMP) END AS Original_Putaway_Datetime, CASE WHEN OriginalPutawayTime IN (\'NA\',\'\') THEN NULL ELSE CAST(CAST(OriginalPutawayTime AS TIMESTAMP) AS DATE) END AS Original_Putaway_Date, OriginalBinWhenPutawayDone, \'CONSCIOUS CHEMIST\' as data_source FROM public.quickshift_conscious_chemist_putaway_report ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            