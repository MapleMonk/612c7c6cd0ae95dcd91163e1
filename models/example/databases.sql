{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table medmongers_db.MAPLEMONK.medmongers_UNICOMMERCE_RETURNS_INTERMEDIATE as SELECT * FROM( SELECT \"Channel Name\" AS marketplace, \"Channel Name\" AS source, \"Display Order Code\" AS order_id, \"Shipping Address Phone\" AS phone, \"Notification Email\" AS email, TO_VARCHAR(TRY_TO_TIMESTAMP(NULLIF(\"Order Date as dd/mm/yyyy hh:MM:ss\", \'\')), \'YYYY-MM-DD\"T\"HH24:MI:SS\') AS order_date, TO_VARCHAR(TRY_TO_TIMESTAMP(NULLIF(\"Return Date\", \'\')), \'YYYY-MM-DD\"T\"HH24:MI:SS\') AS return_date, \"Display Order Code\" AS reference_code, \"Sale Order Status\", \"Sale Order Item Status\", \"Shipping Package Status Code\", \"Shipping Courier Status\", \"Shipping Tracking Status\", CASE WHEN UPPER(\"Shipping Courier Status\") LIKE \'%RETURN%\' THEN \'RETURN\' END AS return_status, CASE WHEN UPPER(\"Shipping Courier Status\") LIKE \'%RETURN%\' THEN \"Shipping Courier\" END AS return_courier, CASE WHEN UPPER(\"Shipping Courier Status\") LIKE \'%RETURN%\' THEN \"Shipping provider\" END AS return_shipping_provider, CASE WHEN UPPER(\"Shipping Courier Status\") LIKE \'%RETURN%\' THEN \"Tracking Number\" END AS return_Tracking_Number, CASE WHEN UPPER(\"Shipping Courier Status\") LIKE \'%RETURN%\' THEN reference_code END AS return_display_code, CASE WHEN UPPER(\"Shipping Courier Status\") LIKE \'%RETURN%\' THEN TRY_TO_DOUBLE(\"Selling Price\") END AS return_sales, CASE WHEN UPPER(\"Shipping Courier Status\") LIKE \'%RETURN%\' THEN 1 Else 0 END AS return_quantity, \"Sale Order Item Code\" AS Sales_Order_Item_Code, \"SKU Name\" AS sku, COALESCE( \"SKU Name\",\"Item Type Name\") AS product_name, ROW_NUMBER() OVER ( PARTITION BY \"Display Order Code\", \"Sale Order Item Code\" ORDER BY TRY_TO_TIMESTAMP(NULLIF(updated, \'\')) DESC ) AS rw FROM medmongers_db.MAPLEMONK.medmongers_db_get_sale_orders fi ) WHERE rw = 1 and return_status is not null;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            