{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ORPAT_DB.MAPLEMONK.ORPAT_FLIPKART_FACT_ITEMS AS select try_to_date(\"Ordered On\",\'dd-mon-yy\') as Ordered_On ,\"Shipment ID\" as Shipment_ID ,\"ORDER ITEM ID\" as ORDER_ITEM_ID ,\"Order Id\" as Order_Id ,\"HSN CODE\" as HSN_CODE ,upper(\"Order State\") as Order_Status ,\"Order Type\" as Order_Type ,\"FSN\" as FSN ,\"SKU\" as SKU ,Upper(Product) PRODUCT ,\"Invoice No.\" as Invoice_No ,try_cast(\"CGST\" as float) as CGST ,try_cast(\"IGST\" as float) as IGST ,try_cast(\"SGST\" as float) as SGST ,coalesce(try_to_date(\"Invoice Date (mm/dd/yy)\", \'mm/dd/yy\'),try_to_date(\"Invoice Date (mm/dd/yy)\", \'mm/dd/yy HH:MI\'), try_to_date(\"Invoice Date (mm/dd/yy)\", \'MON DD, YYYY HH:MI:SS\'), try_to_date(\"Invoice Date (mm/dd/yy)\")) as Invoice_Date ,try_cast(\"Invoice Amount\" as float) as Invoice_Amount ,try_cast(\"Selling Price Per Item\" as float) as Selling_Price_Per_Item ,try_cast(\"Shipping and Handling Charges\" as float) as Shipping_and_Handling_Charges ,try_cast(Quantity as float) as Quantity ,try_Cast(\"Price inc. FKMP Contribution & Subsidy\" as float) as Price_inc_FKMP_Contribution_Subsidy ,Upper(\"Buyer name\") as Buyer_name ,\"Ship to name\" as Ship_to_name ,\"Address Line 1\" as Address_Line_1 ,\"Address Line 2\" as Address_Line_2 ,Upper(City) as City ,Upper(State) as State ,\"PIN Code\" as PIN_Code ,coalesce(try_to_date(\"Dispatch After date\", \'mm/dd/yy\'),try_to_date(\"Dispatch After date\", \'mm/dd/yyyy HH:MI\'), try_to_date(\"Dispatch After date\", \'MON DD, YYYY HH:MI:SS\'), try_to_date(\"Dispatch After date\")) as Dispatch_After_date ,coalesce(try_to_date(\"Dispatch by date\", \'mm/dd/yy\'),try_to_date(\"Dispatch by date\", \'mm/dd/yyyy HH:MI\'), try_to_date(\"Dispatch by date\", \'MON DD, YYYY HH:MI:SS\'), try_to_date(\"Dispatch by date\")) as Dispatch_by_date ,\"Form requirement\" as Form_requirement ,\"Tracking ID\" as Tracking_ID ,try_cast(\"Package Length (cm)\" as float) as Package_Length ,try_cast(\"Package Breadth (cm)\" as float) as Package_Breadth ,try_cast(\"Package Height (cm)\" as float) as Package_Height ,try_cast(\"Package Weight (kg)\" as float) as Package_Weight ,\"Ready to Make\" as Ready_to_Make ,\"With Attachment\" as With_Attachment ,_ab_source_file_last_modified ,_ab_source_file_url from ORPAT_DB.MAPLEMONK.ORPAT_S3_FLIPKART;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ORPAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            