{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.HOX_BLANKO_Tax_consolidated_report as select a.report_type, a.Company_Name, a.seller_gst, case when lower(b.source) = \'amazon\' then \'Amazon FBA\' else \'Amazon EasyShip\' end as marketplace, a.Reference_Code, a.order_type, a.Order_status, a.Invoice_status, a.report_date, ifnull(c.item_count,1) * a.Quantity as QUANTITY, a.SKU, coalesce(c.new_sku , b.common_sku) as component_sku_name, div0(a.Invoice_amount, count(1) over (partition by a.Reference_Code, a.report_date, lower(a.report_type), lower(a.Order_status), a.SKU order by 1)) as Invoice_amount, div0(b.total_sales, count(1) over (partition by a.Reference_Code, a.report_date, lower(a.report_type), lower(a.Order_status), a.SKU order by 1)) as Selling_price, div0(a.absolute_invoice_amount, count(1) over (partition by a.Reference_Code, a.report_date, lower(a.report_type), lower(a.Order_status), a.SKU order by 1)) as absolute_invoice_amount , div0(a.Tax_exclusive_gross, count(1) over (partition by a.Reference_Code, a.report_date, lower(a.report_type), lower(a.Order_status), a.SKU order by 1)) as Tax_exclusive_gross , div0(a.Total_tax_amount, count(1) over (partition by a.Reference_Code, a.report_date, lower(a.report_type), lower(a.Order_status), a.SKU order by 1)) as Total_tax_amount from HOX_DB.MapleMonk.HOX_BLANKO_Amazon_Tax_Fact_Items a left join HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items b on a.Reference_Code = b.order_id and lower(a.SKU) = lower(b.SKU) left join HOX_DB.MAPLEMONK.Easyecom_sku_combo_master c on lower(b.common_sku) = lower(c.\"PARENT SKU\") union all select REPORT_TYPE , COMPANY_NAME , SELLER_GST , MARKETPLACE , REFERENCE_CODE , ORDER_TYPE , ORDER_STATUS , INVOICE_STATUS , REPORT_DATE , QUANTITY , SKU , COMPONENT_SKU_NAME , INVOICE_AMOUNT , SELLING_PRICE , ABSOLUTE_INVOICE_AMOUNT , TAX_EXCLUSIVE_GROSS , TOTAL_TAX_AMOUNT from hox_db.maplemonk.HOX_BLANKO_Easyecom_Tax_Fact_Items where lower(MARKETPLACE) not like \'%amazon%\' ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        