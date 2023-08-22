{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.vahdam_AUS_shipment_event_list as With Charge as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(Principal,0)) Principal, sum(ifnull(Tax,0)) Tax, sum(ifnull(GiftWrap,0)) GiftWrap, sum(ifnull(GiftWrapTax,0)) GiftWrapTax, sum(ifnull(ShippingCharge,0)) ShippingCharge, sum(ifnull(ShippingTax,0)) ShippingTax from (select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, C.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(C.value:ChargeType) = \'principal\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Principal, case when lower(C.value:ChargeType) = \'tax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Tax, case when lower(C.value:ChargeType) = \'giftwrap\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrap, case when lower(C.value:ChargeType) = \'giftwraptax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrapTax, case when lower(C.value:ChargeType) = \'shippingcharge\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingCharge, case when lower(C.value:ChargeType) = \'shippingtax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingTax, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemChargeList)C where lower(A.value:MarketplaceName) = \'amazon.com.au\' ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Fee as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(FBAPerUnitFulfillmentFee,0)) FBAPerUnitFulfillmentFee, sum(ifnull(Commission,0)) Commission, sum(ifnull(GiftWrapChargeback,0)) GiftWrapChargeback, sum(ifnull(ShippingChargeback,0)) ShippingChargeback, sum(ifnull(VariableClosingFee,0)) VariableClosingFee from (select * from ( select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, D.value:\"FeeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:FeeType) = \'fbaperunitfulfillmentfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as FBAPerUnitFulfillmentFee, case when lower(D.value:FeeType) = \'commission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as Commission, case when lower(D.value:FeeType) = \'giftwrapchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as GiftWrapChargeback, case when lower(D.value:FeeType) = \'shippingchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as ShippingChargeback, case when lower(D.value:FeeType) = \'variableclosingfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as VariableClosingFee, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemFeeList)D where lower(A.value:MarketplaceName) = \'amazon.com.au\' ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Quantity as ( select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, sum(ifnull(Quantity,0)) Quantity from (select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, B.value:QuantityShipped as Quantity, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B where lower(A.value:MarketplaceName) = \'amazon.com.au\') where rw = 1) group by 1,2,3,4,5,6 ), Promotion as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(Promotion,0)) Promotion from (select * from ( Select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, C.value:\"PromotionAmount\":\"CurrencyCode\" as Currency, ifnull(C.value:\"PromotionAmount\":\"CurrencyAmount\",0) Promotion, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:PromotionList)C where lower(A.value:MarketplaceName) = \'amazon.com.au\' ) where rw = 1 ) group by 1,2,3,4,5,6,7 ) select replace(coalesce(C.AmazonOrderId,F.AmazonOrderId,Q.AmazonOrderId,P.AmazonOrderId),\'\"\"\',\'\') as AmazonOrderId ,replace(coalesce(C.MarketplaceName,F.MarketplaceName,Q.MarketplaceName,P.MarketplaceName),\'\"\"\',\'\') as MarketplaceName ,replace(coalesce(C.PostedDate,F.PostedDate,Q.PostedDate,P.PostedDate),\'\"\"\',\'\') as PostedDate ,replace(coalesce(C.SellerOrderId,F.SellerOrderId,Q.SellerOrderId,P.SellerOrderId),\'\"\"\',\'\') as SellerOrderId ,replace(coalesce(C.OrderItemId,F.OrderItemId,Q.OrderItemId,P.OrderItemId),\'\"\"\',\'\') as OrderItemId ,replace(coalesce(C.SellerSKU,F.SellerSKU,Q.SellerSKU,P.SellerSKU),\'\"\"\',\'\') as SellerSKU ,replace(coalesce(C.Currency,F.Currency,P.Currency),\'\"\"\',\'\') as Currency ,ifnull(C.Principal,0) Principal ,ifnull(C.Tax,0) Tax ,ifnull(C.GiftWrap,0) GiftWrap ,ifnull(C.GiftWrapTax,0) GiftWrapTax ,ifnull(C.ShippingCharge,0) ShippingCharge ,ifnull(C.ShippingTax,0) ShippingTax ,ifnull(F.FBAPerUnitFulfillmentFee,0) FBAPerUnitFulfillmentFee ,ifnull(F.Commission,0) Commission ,ifnull(F.GiftWrapChargeback,0) GiftWrapChargeback ,ifnull(F.ShippingChargeback,0) ShippingChargeback ,ifnull(F.VariableClosingFee,0) VariableClosingFee ,ifnull(Q.Quantity,0) Quantity ,ifnull(P.Promotion,0) Promotion from CHARGE C full outer join FEE F on C.AmazonOrderId=F.AmazonOrderId and C.OrderItemId=F.OrderItemId and C.PostedDate=F.PostedDate full outer join QUANTITY Q on C.AmazonOrderId=Q.AmazonOrderId and C.OrderItemId=Q.OrderItemId and C.PostedDate=Q.PostedDate full outer join PROMOTION P on C.AmazonOrderId=P.AmazonOrderId and C.OrderItemId=P.OrderItemId and C.PostedDate=P.PostedDate; create or replace table VAHDAM_DB.MAPLEMONK.vahdam_AUS_refund_event_list as With Charge as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,Currency ,sum(ifnull(Tax,0)) Tax ,sum(ifnull(Principal,0)) Principal from (select * from (select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, C.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(C.value:ChargeType) = \'tax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Tax, case when lower(C.value:ChargeType) = \'principal\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Principal, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B, lateral flatten(input => B.value:ItemChargeAdjustmentList)C where lower(A.value:MarketplaceName) = \'amazon.com.au\') where rw = 1 ) group by 1,2,3,4,5,6,7) , Fee as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,Currency ,sum(ifnull(Commission,0)) Commission ,sum(ifnull(RefundCommission,0)) RefundCommission from (select * from (Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, D.value:\"FeeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:FeeType) = \'commission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as Commission, case when lower(D.value:FeeType) = \'refundcommission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as RefundCommission, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input =>B.value:ItemFeeAdjustmentList)D where lower(A.value:MarketplaceName) = \'amazon.com.au\') where rw = 1) group by 1,2,3,4,5,6,7 ) , Quantity as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,sum(ifnull(Quantity,0)) as Quantity from (select * from (Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, B.value:QuantityShipped as Quantity, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B where lower(A.value:MarketplaceName) = \'amazon.com.au\') where rw = 1) group by 1,2,3,4,5,6 ) , Promotion as ( select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,sum(ifnull(Refund_Promotion,0)) as Refund_Promotion from ( select * from ( Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, P.value:\"PromotionAmount\":\"CurrencyAmount\" as Refund_Promotion, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_aus_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input => B.value:PromotionAdjustmentList)P where lower(A.value:MarketplaceName) = \'amazon.com.au\' ) where rw = 1 ) group by 1,2,3,4,5,6 ) select replace(coalesce(C.AmazonOrderID,F.AmazonOrderID,Q.AmazonOrderID,P.AmazonOrderID),\'\"\"\',\'\') as AmazonOrderID ,replace(coalesce(C.MarketplaceName,F.MarketplaceName,Q.MarketplaceName,P.MarketplaceName),\'\"\"\',\'\') as MarketplaceName ,replace(coalesce(C.PostedDate,F.PostedDate,Q.PostedDate,P.PostedDate),\'\"\"\',\'\') as PostedDate ,replace(coalesce(C.SellerOrderId,F.SellerOrderId,Q.SellerOrderId,P.SellerOrderId),\'\"\"\',\'\') as SellerOrderId ,replace(coalesce(C.SellerSKU,F.SellerSKU,Q.SellerSKU,P.SellerSKU),\'\"\"\',\'\') SellerSKU ,replace(coalesce(C.OrderAdjustmentItemId,F.OrderAdjustmentItemId,Q.OrderAdjustmentItemId,P.OrderAdjustmentItemId),\'\"\"\',\'\') OrderAdjustmentItemId ,ifnull(C.Tax,0) Tax ,ifnull(C.Principal,0) Principal ,ifnull(F.Commission,0) Commission ,ifnull(F.RefundCommission,0) RefundCommission ,ifnull(Q.Quantity,0) Quantity ,ifnull(P.Refund_Promotion,0) Refund_Promotion from Charge C full outer join Fee F on C.AmazonOrderID=F.AmazonOrderID and C.OrderAdjustmentItemId = F.OrderAdjustmentItemId and C.PostedDate = F.PostedDate full outer join Quantity Q on C.AmazonOrderID=Q.AmazonOrderID and C.OrderAdjustmentItemId = Q.OrderAdjustmentItemId and C.PostedDate = Q.PostedDate full outer join Promotion P on C.AmazonOrderID=P.AmazonOrderID and C.OrderAdjustmentItemId = P.OrderAdjustmentItemId and C.PostedDate = P.PostedDate; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_AUS_MONTHLY_STORAGE_FEES_PR AS WITH STORAGECOST AS (WITH DATES AS ( with RECURSIVE date_range AS ( SELECT TO_DATE(\'2023-04-01\') AS date_value UNION ALL SELECT date_value + INTERVAL \'1 day\' FROM date_range WHERE date_value + INTERVAL \'1 day\' <= \'2024-03-31\' ) SELECT date_value FROM date_range), STORAGE AS (select date,replace(fee_type,\'\"\"\',\'\') Fee_type,replace(CURRENCY,\'\"\"\',\'\') CURRENCY, sum(amount) Amount from ( select *, dense_rank() over (partition by order_id,date order by date) rw from (select date,servicefeeeventlist,A.value:AmazonOrderId Order_id,B.value:FeeType Fee_type,B.value:\"FeeAmount\":\"CurrencyCode\" CURRENCY, sum(B.value:\"FeeAmount\":\"CurrencyAmount\") Amount from (select date, servicefeeeventlist from (select servicefeeeventlist,_airbyte_emitted_at::date Date, row_number() over (partition by servicefeeeventlist order by _airbyte_emitted_at::date asc) rw from vahdam_db.maplemonk.casp_AUS_listfinancialevents) where rw = 1 and lower(servicefeeeventlist) like \'%storage%\' ) ,LATERAL FLATTEN (INPUT => servicefeeeventlist)A ,LATERAL FLATTEN (INPUT => A.VALUE:FeeList) B group by 1,2,3,4,5)) where rw = 1 and lower(fee_type) like \'%storage%\' AND CURRENCY = \'AUD\' group by 1,2,3) SELECT COALESCE(D.DATE_VALUE,S.DATE) AS DATE, IFNULL(S.FEE_TYPE,\'NA\') FEE_TYPE, IFNULL(S.CURRENCY,\'NA\') CURRENCY,SUM(IFNULL(S.AMOUNT,0)) AMOUNT FROM DATES D LEFT JOIN STORAGE S ON D.DATE_VALUE=S.DATE GROUP BY 1,2,3) SELECT MONTH,YEAR,SUM(IFNULL(STFS_CURRENT_MONTH,0)) STFS_CURRENT_MONTH, SUM(IFNULL(LTFS_CURRENT_MONTH,0)) LTFS_CURRENT_MONTH FROM (SELECT MONTH(DATE) AS MONTH, YEAR(DATE) AS YEAR, CASE WHEN FEE_TYPE = \'FBAStorageFee\' THEN SUM(IFNULL(AMOUNT,0)) ELSE 0 END AS STFS_CURRENT_MONTH ,CASE WHEN FEE_TYPE = \'FBALongTermStorageFee\' THEN SUM(IFNULL(AMOUNT,0)) ELSE 0 END AS LTFS_CURRENT_MONTH FROM STORAGECOST GROUP BY FEE_TYPE,1,2 ORDER BY 2,1) GROUP BY 1,2 ORDER BY 2,1;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        