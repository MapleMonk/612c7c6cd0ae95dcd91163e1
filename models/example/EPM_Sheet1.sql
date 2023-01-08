{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (LogDate Date); UPDATE eggozdb.maplemonk.epm_sheet1 SET LogDate = TRY_TO_DATE(\"DATE\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (Pdate Date); UPDATE eggozdb.maplemonk.epm_sheet1 SET Pdate = TRY_TO_DATE(\"Processing Date\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_pkg_loss ADD (LogDate Date); UPDATE eggozdb.maplemonk.epm_pkg_loss SET LogDate = TRY_TO_DATE(\"Date \",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_pkg_loss ADD (Pdate Date); UPDATE eggozdb.maplemonk.epm_pkg_loss SET Pdate = TRY_TO_DATE(\"Packed Date\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_kpi_tracker_v2 ADD (Pdate Date); UPDATE eggozdb.maplemonk.epm_kpi_tracker_v2 SET Pdate = TRY_TO_DATE(\"Date\",\'DD/MM/YYYY\'); create or replace table eggozdb.maplemonk.ncr_epm_ppp as select pdate, category, ifnull(procured_eggs,0) procured_eggs, ifnull(processed_eggs,0) processed_eggs, ifnull(clean_eggs,0) clean_eggs, ifnull(processing_ub,0) processing_ub, ifnull(processing_loss,0) processing_loss, ifnull(packaging_loss,0) packaging_loss, ifnull(packaging_ub,0) packaging_ub, ifnull(pending_eggs_to_pack,0) pending_eggs_to_pack from ( select processing.pdate, processing.category, processing.procured_eggs, processing.processed_eggs, processing.clean_eggs, processing.processing_ub, processing.processing_loss, packaging.packaging_loss, packaging.packaging_ub, packaging.pending_eggs_to_pack from ( select pdate, category, sum(\"Total eggs\") as procured_eggs, sum(\"Processed Eggs\") as processed_eggs, sum(\"Total Clean Eggs\") as clean_eggs, sum(ub) as processing_ub, sum(loss) as processing_loss from eggozdb.maplemonk.epm_sheet1 where pdate >= \'2022-08-01\' and pdate is not null group by pdate, category order by pdate desc ) processing full outer join ( select pdate, category, sum(loss) packaging_loss, sum(\"Packaging UB\") as packaging_ub, sum(\"Pending Eggs\") pending_eggs_to_pack from eggozdb.maplemonk.epm_pkg_loss where pdate >= \'2022-08-01\' and pdate is not null group by pdate, category order by pdate ) packaging on processing.pdate = packaging.pdate and processing.category = packaging.category ) where pdate is not null;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.EPM_Sheet1
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        