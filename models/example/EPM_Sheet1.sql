{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (LogDate Date); UPDATE eggozdb.maplemonk.epm_sheet1 SET LogDate = TRY_TO_DATE(\"DATE\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (Pdate Date); UPDATE eggozdb.maplemonk.epm_sheet1 SET Pdate = TRY_TO_DATE(\"Processing Date\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_pkg_loss ADD (LogDate Date); UPDATE eggozdb.maplemonk.epm_pkg_loss SET LogDate = TRY_TO_DATE(\"Date \",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_pkg_loss ADD (Pdate Date); UPDATE eggozdb.maplemonk.epm_pkg_loss SET Pdate = TRY_TO_DATE(\"Packed Date\",\'DD/MM/YYYY\'); create or replace table as eggozdb.maplemonk.ncr_epm_ppp as select * from ( select processing.pdate, processing.category, processing.procured_eggs, processing.processed_eggs, processing.clean_eggs, processing.processing_ub, processing.processing_loss, packaging.packaging_loss, packaging.packaging_ub, packaging.pending_eggs_to_pack from ( select pdate, category, sum(\"Total eggs\") as procured_eggs, sum(\"Processed Eggs\") as processed_eggs, sum(\"Total Clean Eggs\") as clean_eggs, sum(ub) as processing_ub, sum(loss) as processing_loss from eggozdb.maplemonk.epm_sheet1 where pdate >= \'2022-08-01\' and pdate is not null group by pdate, category order by pdate desc ) processing full outer join ( select pdate, category, sum(loss) packaging_loss, sum(\"Packaging UB\") as packaging_ub, sum(\"Pending Eggs\") pending_eggs_to_pack from eggozdb.maplemonk.epm_pkg_loss where pdate >= \'2022-08-01\' and pdate is not null group by pdate, category order by pdate ) packaging on processing.pdate = packaging.pdate and processing.category = packaging.category ) where pdate is not null;",
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
                        