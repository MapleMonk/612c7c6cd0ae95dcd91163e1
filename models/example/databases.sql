{{ config(
            materialized='table',
                post_hook={
                    "sql": "-- BigQuery script to normalize Location across three tables DECLARE target_tables ARRAY<STRING> = [ \'kerala-ayurveda-wh.MapleMonk.KA_S3_tally_inventory\', \'kerala-ayurveda-wh.MapleMonk.KA_S3_tally_inventory_scd\', \'kerala-ayurveda-wh.MapleMonk.KA_S3_tally_compliance\', \'kerala-ayurveda-wh.MapleMonk.KA_S3_tally_compliance_scd\' ]; FOR table_name IN UNNEST(target_tables) DO EXECUTE IMMEDIATE FORMAT(\"\"\" UPDATE `%s` SET Location = CASE Location WHEN \'KAL WhitefieldKALPAM\' THEN \'KAL Whitefield\' WHEN \'KAL WTFKALPAM2\' THEN \'KAL Whitefield\' WHEN \'DC KA BLR Whitefield\' THEN \'KAL Whitefield\' WHEN \'DC KA BLR Indiranagar\' THEN \'KAL Indira Nagar\' WHEN \'DC KA BLR Jayanagar\' THEN \'KAL Jayanagar\' WHEN \'DC KA BLR HSR\' THEN \'KAL HSR Wellness Centre\' WHEN \'DC KA BLR Shivaji Nagar\' THEN \'KAL Shivajinagar Clinic\' WHEN \'DC KA BLR Koramangala\' THEN \'KAL Koramangala\' WHEN \'DC KL KOC Aluva\' THEN \'KAL Hospital Aluva\' WHEN \'DC KL KOC Edapally\' THEN \'KAL Edapally\' WHEN \'DC KL KOC Vallanjambalam\' THEN \'KAL Ernakulam\' WHEN \'DC KL TVM Kowdiar\' THEN \'KAL Kowdiar\' WHEN \'DC TN CHN Annanagar\' THEN \'KAL Anna Nagar\' WHEN \'DC DL Green Park\' THEN \'KAL Green Park\' WHEN \'DC MH Pune\' THEN \'KAL Pune\' WHEN \'DC MH Mum Marol\' THEN \'KAL Marol\' WHEN \'DC MH Mum MHADA\' THEN \'KAL Lokhandwala\' WHEN \'DC TG HYD Somajiguda\' THEN \'KAL Somajiguda\' WHEN \'DC TG VZG\' THEN \'KAL Visakhapatnam\' WHEN \'IP KL Kasargod\' THEN \'KAL Kasargode\' WHEN \'IP KL Mannipady\' THEN \'KAL Kasargode Hospital Madhur\' ELSE Location END WHERE Location IN UNNEST([ \'KAL WhitefieldKALPAM\',\'KAL WTFKALPAM2\', \'DC KA BLR Whitefield\',\'DC KA BLR Indiranagar\', \'DC KA BLR Jayanagar\',\'DC KA BLR HSR\', \'DC KA BLR Shivaji Nagar\',\'DC KA BLR Koramangala\', \'DC KL KOC Aluva\',\'DC KL KOC Edapally\', \'DC KL KOC Vallanjambalam\',\'DC KL TVM Kowdiar\', \'DC TN CHN Annanagar\', \'DC DL Green Park\', \'DC MH Pune\',\'DC MH Mum Marol\',\'DC MH Mum MHADA\', \'DC TG HYD Somajiguda\',\'DC TG VZG\', \'IP KL Kasargod\',\'IP KL Mannipady\' ]); \"\"\", table_name); END FOR;",
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
            