{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `maplemonk.plaeup_leadsquared_leads_fact_items` AS SELECT CAST(ProspectID AS STRING) AS lead_id, ProspectAutoId AS lead_auto_id, EmailAddress AS email, Phone AS phone_number, mx_City AS city, mx_State AS state, mx_Zone AS zone, mx_Country AS country, SAFE_CAST(Score AS INT64) AS lead_score, SAFE_CAST(EngagementScore AS INT64) AS engagement_score, SAFE_CAST(QualityScore01 AS INT64) AS quality_score, ProspectStage AS current_stage, SAFE_CAST(mx_Win_Probability AS FLOAT64) AS win_probability_pct, Source AS lead_source, SourceMedium AS lead_medium, SourceCampaign AS campaign_name, Origin AS lead_origin, SAFE_CAST(mx_Number_of_Schools AS INT64) AS total_schools, SAFE_CAST(mx_Total_Student_Strength AS INT64) AS student_capacity, SAFE_CAST(mx_Approx_Tuition_Fees AS FLOAT64) AS potential_revenue, OwnerId AS owner_id, OwnerIdEmailAddress AS owner_email, TIMESTAMP(CreatedOn) AS created_at, TIMESTAMP(ModifiedOn) AS updated_at, TIMESTAMP(LeadConversionDate) AS converted_at, DATE_DIFF(CURRENT_DATE(), DATE(TIMESTAMP(CreatedOn)), DAY) AS lead_age_days FROM `plaeup-wh.maplemonk.leadsquared_get_lead_info`; CREATE OR REPLACE TABLE `maplemonk.plaeup_leadsquared_opportunities_fact_items` AS SELECT CAST(OpportunityId AS STRING) AS opportunity_id, CAST(RelatedProspectId AS STRING) AS opp_lead_id, upper(Leadname) as Lead_Name, phone as Lead_Contact, emailaddress as lead_email, Status AS opp_status, StatusReason AS opp_reason, mx_custom_2 as current_status, SAFE_CAST(mx_Custom_1 AS FLOAT64) AS deal_value, SAFE_CAST(PropensityScore AS INT64) AS conversion_propensity, OwnerName AS sales_rep_name, COALESCE(SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %I:%M:%S %p\', CreatedOn),SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E*S\', CreatedOn)) AS opp_created_at, COALESCE(SAFE.PARSE_TIMESTAMP(\'%m/%d/%Y %I:%M:%S %p\', ModifiedOn),SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%E*S\', ModifiedOn)) AS opp_modified_at, SAFE_CAST(OpportunityAge AS INT64) AS days_in_pipeline, RecommendedActionCode AS next_step_action, KeyPropensityInsights AS ai_insights FROM `maplemonk.leadsquared_get_oppurtunity_by_lead_id`; create or replace table maplemonk.plaeup_leadsquared_leads_and_opportunities_funnel as SELECT l.*, o.*, CASE WHEN l.engagement_score > 50 AND o.opp_status = \'Open\' THEN 1 ELSE 0 END AS is_hot_lead FROM `maplemonk.plaeup_leadsquared_leads_fact_items` l LEFT JOIN (select * from `maplemonk.plaeup_leadsquared_opportunities_fact_items` qualify row_number() over (partition by opp_lead_id order by opp_modified_at desc) =1 ) o ON l.lead_id = o.opp_lead_id;",
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
            