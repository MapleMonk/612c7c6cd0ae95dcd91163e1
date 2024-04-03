{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table JPEARLS_DB.MAPLEMONK.JPEARLS_DB_RETAIL_STORE_FACT_ITEMS AS select try_to_date(DATE,\'dd/mm/yyyy\') DATE ,upper(Branch_Name) PRE_SHOP_NAME ,upper(FINAL_BRANCH) SHOP_NAME ,\'RETAIL STORES\' MARKETPLACE ,UPPER(Designed_By) DESIGNED_BY ,Upper(Area) AREA ,UPPER(City) CITY ,UPPER(Item) PRODUCT_NAME ,UPPER(SHAPE) PRODUCT_SHAPE ,UPPER(COLOUR) PRODUCT_COLOUR ,Sku_Code SKU_CODE ,Bar_Code BAR_CODE ,Upper(Category) CATEGORY ,upper(Item_Type) ITEM_TYPE ,upper(Supplier) SUPPLIER ,Doc_No DOC_NO ,UPPER(LENGTH) LENGTH ,try_cast(replace(PURITY,\',\',\'\') as float) PURITY ,SERIES ,Upper(NARRATION) NARRATION ,Upper(Salesman) SALESMAN ,upper(Customer) CUSTOMER_NAME ,Mobile_No PHONE ,QUALITY ,SIZE ,try_cast(replace(Stn_Wt,\',\',\'\') as float) STN_WEIGHT ,try_cast(replace(Net_Wt,\',\',\'\') as float) NET_WEIGHT ,try_cast(replace(Gross_Wt,\',\',\'\') as float) GROSS_WEIGHT ,try_cast(replace(\"Dia_Wt._Ct\",\',\',\'\') as float) DIA_WEIGHT ,try_cast(replace(Pcs,\',\',\'\') as integer) QUANTITY ,try_cast(replace(Rate,\',\',\'\') as float) RATE ,try_cast(replace(Gst,\',\',\'\') as float) GST ,try_cast(replace(Mrp_Amount,\',\',\'\') as float) MRP ,try_cast(replace(Dia_Amt,\',\',\'\') as float) DIA_AMOUNT ,try_cast(replace(\"Stn_Amt.\",\',\',\'\') as float) STN_AMOUNT ,try_cast(replace(Taxable_Amt,\',\',\'\') as float) TAXABLE_AMOUNT ,try_cast(replace(Item_Taxable_Amount,\',\',\'\') as float) ITEM_TAXABLE_AMOUNT ,try_cast(replace(Netamount,\',\',\'\') as float) NET_AMOUNT ,try_cast(replace(Net_sales_Amount,\',\',\'\') as float) NET_SALES_AMOUNT ,Card_Payment CARD_PAYMENT ,Cash_Payment CASH_PAYMENT ,Purchase_Cost PURCHASE_COST ,Other_Payment OTHER_PAYMENT ,Purchase_Type PURCHASE_TYPE ,Credit_Payment CREDIT_PAYMENT ,try_cast(replace(Labour_Charges,\',\',\'\') as Float) LABOUR_CHARGES ,Label_date_range LABEL_DATE_RANGE ,Label_Out_location LABEL_OUT_LOCATION ,try_to_Date(Original_Labeli_ng_Date,\'dd/mm/yyyy\') ORIGINAL_LABEL_DATE ,Label_Age_Uptod_Documentdate::integer LABEL_AGE_UPTO_DOCUMENT_DATE from jpearls_db.maplemonk.jpearls_2022_retail_orders_data where date is not null union all select try_to_date(DATE,\'dd-mm-yyyy\') DATE ,upper(\"Branch Name\") PRE_SHOP_NAME ,upper(\"Final Branch\") SHOP_NAME ,\'RETAIL STORES\' MARKETPLACE ,UPPER(\"Designed By\") DESIGNED_BY ,Upper(\"Area \") AREA ,UPPER(\"City \") CITY ,UPPER(\"Item \") PRODUCT_NAME ,UPPER(\"SHAPE\") PRODUCT_SHAPE ,UPPER(\"COLOUR\") PRODUCT_COLOUR ,\"Sku Code\" SKU_CODE ,\"Bar Code \" BAR_CODE ,Upper(\"Category \") CATEGORY ,upper(\"Item Type \") ITEM_TYPE ,upper(\"Supplier \") SUPPLIER ,\"Doc No\" DOC_NO ,UPPER(LENGTH) LENGTH ,try_cast(replace(PURITY,\',\',\'\') as float) PURITY ,SERIES ,Upper(NARRATION) NARRATION ,Upper(\"Salesman \") SALESMAN ,upper(\"Customer \") CUSTOMER_NAME ,\"Mobile No\" PHONE ,QUALITY ,SIZE ,try_cast(replace(\"Stn Wt \",\',\',\'\') as float) STN_WEIGHT ,try_cast(replace(\"Net Wt \",\',\',\'\') as float) NET_WEIGHT ,try_cast(replace(\"Gross Wt \",\',\',\'\') as float) GROSS_WEIGHT ,try_cast(replace(\"Dia Wt. Ct \",\',\',\'\') as float) DIA_WEIGHT ,try_cast(replace(\"Pcs \",\',\',\'\') as integer) QUANTITY ,try_cast(replace(\"Rate \",\',\',\'\') as float) RATE ,try_cast(replace(\"Gst \",\',\',\'\') as float) GST ,try_cast(replace(\"Mrp Amount \",\',\',\'\') as float) MRP ,try_cast(replace(\"Dia Amt \",\',\',\'\') as float) DIA_AMOUNT ,try_cast(replace(\"Stn Amt. \",\',\',\'\') as float) STN_AMOUNT ,try_cast(replace(\"Taxable Amt \",\',\',\'\') as float) TAXABLE_AMOUNT ,try_cast(replace(\"Item Taxable Am Ount\",\',\',\'\') as float) ITEM_TAXABLE_AMOUNT ,try_cast(replace(\"Netamount \",\',\',\'\') as float) NET_AMOUNT ,try_cast(replace(\"Net sales Amount\",\',\',\'\') as float) NET_SALES_AMOUNT ,\"Card Payment \" CARD_PAYMENT ,\"Cash Payment \" CASH_PAYMENT ,\"Purchase Cost\" PURCHASE_COST ,\"Other Payment \" OTHER_PAYMENT ,\"Purchase Type \" PURCHASE_TYPE ,\"Credit Payment \" CREDIT_PAYMENT ,try_cast(replace(\"Labour Charges \",\',\',\'\') as Float) LABOUR_CHARGES ,\"Label date range\" LABEL_DATE_RANGE ,\"Label Out location\" LABEL_OUT_LOCATION ,try_to_Date(\"Original Labeli Ng Date\",\'dd/mm/yyyy\') ORIGINAL_LABEL_DATE ,\"Label Age Uptod Ocumentdate\"::integer LABEL_AGE_UPTO_DOCUMENT_DATE from jpearls_db.maplemonk.jpearls_retail_orders_data where date is not null union all select try_to_date(DATE,\'dd-mm-yyyy\') DATE ,upper(\"Branch Name\") PRE_SHOP_NAME ,upper(\"Final Branch\") SHOP_NAME ,\'RETAIL STORES\' MARKETPLACE ,UPPER(\"Designed By\") DESIGNED_BY ,Upper(\"Area \") AREA ,UPPER(\"City \") CITY ,UPPER(\"Item \") PRODUCT_NAME ,UPPER(\"SHAPE\") PRODUCT_SHAPE ,UPPER(\"COLOUR\") PRODUCT_COLOUR ,\"Sku Code\" SKU_CODE ,\"Bar Code \" BAR_CODE ,Upper(\"Category \") CATEGORY ,upper(\"Item Type \") ITEM_TYPE ,upper(\"Supplier \") SUPPLIER ,\"Doc No\" DOC_NO ,UPPER(LENGTH) LENGTH ,try_cast(replace(PURITY,\',\',\'\') as float) PURITY ,SERIES ,Upper(NARRATION) NARRATION ,Upper(\"Salesman \") SALESMAN ,upper(\"Customer \") CUSTOMER_NAME ,\"Mobile No\" PHONE ,QUALITY ,SIZE ,try_cast(replace(\"Stn Wt \",\',\',\'\') as float) STN_WEIGHT ,try_cast(replace(\"Net Wt \",\',\',\'\') as float) NET_WEIGHT ,try_cast(replace(\"Gross Wt \",\',\',\'\') as float) GROSS_WEIGHT ,try_cast(replace(\"Dia Wt. Ct \",\',\',\'\') as float) DIA_WEIGHT ,try_cast(replace(\"Pcs \",\',\',\'\') as integer) QUANTITY ,try_cast(replace(\"Rate \",\',\',\'\') as float) RATE ,try_cast(replace(\"Gst \",\',\',\'\') as float) GST ,try_cast(replace(\"Mrp Amount \",\',\',\'\') as float) MRP ,try_cast(replace(\"Dia Amt \",\',\',\'\') as float) DIA_AMOUNT ,try_cast(replace(\"Stn Amt. \",\',\',\'\') as float) STN_AMOUNT ,try_cast(replace(\"Taxable Amt \",\',\',\'\') as float) TAXABLE_AMOUNT ,try_cast(replace(\"Item Taxable Am Ount\",\',\',\'\') as float) ITEM_TAXABLE_AMOUNT ,try_cast(replace(\"Netamount \",\',\',\'\') as float) NET_AMOUNT ,try_cast(replace(\"Net sales Amount\",\',\',\'\') as float) NET_SALES_AMOUNT ,\"Card Payment \" CARD_PAYMENT ,\"Cash Payment \" CASH_PAYMENT ,\"Purchase Cost\" PURCHASE_COST ,\"Other Payment \" OTHER_PAYMENT ,\"Purchase Type \" PURCHASE_TYPE ,\"Credit Payment \" CREDIT_PAYMENT ,try_cast(replace(\"Labour Charges \",\',\',\'\') as Float) LABOUR_CHARGES ,\"Label date range\" LABEL_DATE_RANGE ,\"Label Out location\" LABEL_OUT_LOCATION ,try_to_Date(\"Original Labeli Ng Date\",\'dd/mm/yyyy\') ORIGINAL_LABEL_DATE ,\"Label Age Uptod Ocumentdate\"::integer LABEL_AGE_UPTO_DOCUMENT_DATE from jpearls_db.maplemonk.jpearls_2024_retail_orders_data where date is not null;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from JPEARLS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        