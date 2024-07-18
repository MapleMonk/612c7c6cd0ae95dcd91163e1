{{ config( materialized='table' ) }}
            select * from maplemonk.nonSub_customers;