{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.returns_dto AS WITH dto AS ( SELECT \"Warehouse Name\", \"Tracking No\", \"Sale Order Item Code\", \"Original Sale Order Code\", \"Courier Provider Name\", \"Return Item Status\", \"Channel Return Created Date\", \"Reverse Pickup Status\", \"Reverse Pickup Last Updated\", \"Reverse Pickup Created\", \"Return Delivery Date\", \"Putaway Code\", \"Putaway Status\", \"Putaway Created By\", \"Putaway Last Updated\", \"Reverse Pickup No\", \"Item Name\", \"QC Comment\", \"Return Reason\", \"Channel Name\", \"Item SkuCode\", \"Shipping Courier Status\", \"Shipping Tracking Status\", \"Reverse Pickup Created By\", \"Total Received Items\" FROM snitch_db.maplemonk.snitch_get_copy_of_reverse_pickup ), clickpost_db AS ( SELECT awb, \"RTO AWB\", \"RTO Mark Date\", \"Order ID\", \"Drop City\", \"Order Date\", \"Created at\", \"Pickup Date\", \"Delivery Date\", \"Drop Name\", \"Updated at\", \"Items Quantity\", \"RVP Reason\", \"Return Name\", \"Latest Timestamp\", \"Clickpost Unified Status\", \"Product SKU Code\", \"Courier Partner\" from( SELECT awb, \"RTO AWB\", \"RTO Mark Date\", \"Order ID\", \"Drop City\", \"Order Date\", \"Created at\", \"Pickup Date\", \"Delivery Date\", \"Drop Name\", \"Updated at\", \"Items Quantity\", \"RVP Reason\", \"Return Name\", \"Latest Timestamp\", \"Clickpost Unified Status\", \"Product SKU Code\", \"Courier Partner\", ROW_NUMBER() OVER (PARTITION BY awb ORDER BY \"Latest Timestamp\" DESC) AS row_num FROM snitch_db.maplemonk.snitch_clickpost_track_order_dashboard_report) where row_num = 1 ) SELECT dto.*, cp.*, CASE WHEN \"Channel Name\" LIKE \'%MYNTRA%\' THEN \'MYNTRA\' WHEN \"Channel Name\" LIKE \'%AJIO%\' THEN \'AJIO\' WHEN \"Channel Name\" LIKE \'%AMAZON%\' THEN \'AMAZON\' WHEN \"Channel Name\" LIKE \'%FLIPKART%\' THEN \'FLIPKART\' ELSE \"Channel Name\" END AS marketplace_mapped, ROW_NUMBER() OVER ( PARTITION BY dto.\"Sale Order Item Code\" ORDER BY COALESCE(cp.\"Latest Timestamp\", dto.\"Reverse Pickup Last Updated\") DESC ) AS row_num, CASE WHEN cp.\"Delivery Date\" IS NULL AND dto.\"Return Delivery Date\" IS NULL THEN NULL WHEN cp.\"Delivery Date\" = \'\' AND dto.\"Return Delivery Date\" = \'\' THEN NULL ELSE COALESCE( TRY_TO_TIMESTAMP(NULLIF(cp.\"Delivery Date\", \'\'), \'YYYY-MM-DD HH24:MI:SS\'), TRY_TO_TIMESTAMP(NULLIF(dto.\"Return Delivery Date\", \'\'), \'YYYY-MM-DD HH24:MI:SS\') ) END AS Final_Delivery_date FROM dto LEFT JOIN clickpost_db cp ON dto.\"Tracking No\" = cp.awb",
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
            