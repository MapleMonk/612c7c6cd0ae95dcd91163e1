{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.pkg_loss ADD (cdate Date); UPDATE eggozdb.maplemonk.pkg_loss SET cdate = TRY_TO_DATE(\"Date \",\'dd-Mon-yyyy\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.Pkg_Loss
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        