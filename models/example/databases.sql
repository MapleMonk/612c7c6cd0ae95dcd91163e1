{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.vahdam_IN_shipment_event_list as With Tax as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(ItemTDS,0)) ItemTDS from (select * from (Select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, D.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:ChargeType) = \'itemtds\' then D.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end ItemTDS, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemTaxWithheldList)C ,lateral flatten(input =>C.value:TaxesWithheld)D where lower(A.value:MarketplaceName) = \'amazon.in\' ) where rw = 1) group by 1,2,3,4,5,6,7 ), Promotion as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(Promotion,0)) Promotion from (select * from ( Select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, C.value:\"PromotionAmount\":\"CurrencyCode\" as Currency, ifnull(C.value:\"PromotionAmount\":\"CurrencyAmount\",0) Promotion, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:PromotionList)C where lower(A.value:MarketplaceName) = \'amazon.in\' ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Charge as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(Principal,0)) Principal, sum(ifnull(Tax,0)) Tax, sum(ifnull(GiftWrap,0)) GiftWrap, sum(ifnull(GiftWrapTax,0)) GiftWrapTax, sum(ifnull(ShippingCharge,0)) ShippingCharge, sum(ifnull(ShippingTax,0)) ShippingTax, sum(ifnull(TCS_IGST,0)) \"TCS-IGST\", sum(ifnull(TCS_CGST,0)) \"TCS-CGST\", sum(ifnull(TCS_SGST,0)) \"TCS-SGST\" from (select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, C.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(C.value:ChargeType) = \'principal\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Principal, case when lower(C.value:ChargeType) = \'tax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Tax, case when lower(C.value:ChargeType) = \'giftwrap\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrap, case when lower(C.value:ChargeType) = \'giftwraptax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrapTax, case when lower(C.value:ChargeType) = \'shippingcharge\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingCharge, case when lower(C.value:ChargeType) = \'shippingtax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingTax, case when lower(C.value:ChargeType) = \'tcs-igst\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as TCS_IGST, case when lower(C.value:ChargeType) = \'tcs-cgst\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as TCS_CGST, case when lower(C.value:ChargeType) = \'tcs-sgst\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as TCS_SGST, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemChargeList)C where lower(A.value:MarketplaceName) = \'amazon.in\' ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Fee as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(FBAPerUnitFulfillmentFee,0)) FBAPerUnitFulfillmentFee, sum(ifnull(FBAWeightBasedFee,0)) FBAWeightBasedFee, sum(ifnull(Commission,0)) Commission, sum(ifnull(FixedclosingFee,0)) FixedclosingFee, sum(ifnull(GiftWrapChargeback,0)) GiftWrapChargeback, sum(ifnull(ShippingChargeback,0)) ShippingChargeback, sum(ifnull(VariableClosingFee,0)) VariableClosingFee, sum(ifnull(TechnologyFee,0)) TechnologyFee from (select * from ( select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, D.value:\"FeeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:FeeType) = \'fbaperunitfulfillmentfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as FBAPerUnitFulfillmentFee, case when lower(D.value:FeeType) = \'fbaweightbasedfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as FBAWeightBasedFee, case when lower(D.value:FeeType) = \'commission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as Commission, case when lower(D.value:FeeType) = \'fixedclosingfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as FixedclosingFee, case when lower(D.value:FeeType) = \'giftwrapchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as GiftWrapChargeback, case when lower(D.value:FeeType) = \'shippingchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as ShippingChargeback, case when lower(D.value:FeeType) = \'variableclosingfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as VariableClosingFee, case when lower(D.value:FeeType) = \'technologyfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as TechnologyFee, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemFeeList)D where lower(A.value:MarketplaceName) = \'amazon.in\' ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Quantity as ( select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, sum(ifnull(Quantity,0)) Quantity from (select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, B.value:QuantityShipped as Quantity, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B where lower(A.value:MarketplaceName) = \'amazon.in\') where rw = 1) group by 1,2,3,4,5,6 ) , TCM as (select AmazonOrderId ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderItemId ,SellerSKU ,TaxCollectionModel from ( select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, E.value:TaxCollectionModel as TaxCollectionModel, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemTaxWithheldList)E where lower(A.value:MarketplaceName) = \'amazon.in\') where rw = 1) ) select replace(coalesce(C.AmazonOrderId,F.AmazonOrderId,Q.AmazonOrderId,T.AmazonOrderId,P.AmazonOrderId,Z.AmazonOrderId),\'\"\"\',\'\') as AmazonOrderId ,replace(coalesce(C.MarketplaceName,F.MarketplaceName,Q.MarketplaceName,T.MarketplaceName,P.MarketplaceName,Z.MarketplaceName),\'\"\"\',\'\') as MarketplaceName ,replace(coalesce(C.PostedDate,F.PostedDate,Q.PostedDate,T.PostedDate,P.PostedDate,Z.PostedDate),\'\"\"\',\'\') as PostedDate ,replace(coalesce(C.SellerOrderId,F.SellerOrderId,Q.SellerOrderId,T.SellerOrderId,P.SellerOrderId,Z.SellerOrderId),\'\"\"\',\'\') as SellerOrderId ,replace(coalesce(C.OrderItemId,F.OrderItemId,Q.OrderItemId,T.OrderItemId,P.OrderItemId,Z.OrderItemId),\'\"\"\',\'\') as OrderItemId ,replace(coalesce(C.SellerSKU,F.SellerSKU,Q.SellerSKU,T.SellerSKU,P.SellerSKU,Z.SellerSKU),\'\"\"\',\'\') as SellerSKU ,replace(coalesce(C.Currency,F.Currency,P.Currency,Z.Currency),\'\"\"\',\'\') as Currency ,Q.Quantity ,ifnull(P.Promotion,0) Promotion ,C.Principal ,C.Tax ,C.GiftWrap ,C.GiftWrapTax ,C.ShippingCharge ,C.ShippingTax ,C.\"TCS-IGST\" ,C.\"TCS-CGST\" ,C.\"TCS-SGST\" ,ifnull(Z.ItemTDS,0) ItemTDS ,F.FBAPerUnitFulfillmentFee ,F.FBAWeightBasedFee ,F.Commission ,F.FixedclosingFee ,F.GiftWrapChargeback ,F.ShippingChargeback ,F.VariableClosingFee ,F.TechnologyFee ,T.TaxCollectionModel from Charge C left join Fee F on C.AmazonOrderID=F.AmazonOrderID and C.OrderItemId = F.OrderItemId and C.PostedDate = F.PostedDate left join Quantity Q on C.AmazonOrderID=Q.AmazonOrderID and C.OrderItemId = Q.OrderItemId and C.PostedDate = Q.PostedDate left join TCM T on C.AmazonOrderID=T.AmazonOrderID and C.OrderItemId = T.OrderItemId and C.PostedDate = T.PostedDate left join Promotion P on C.AmazonOrderID=P.AmazonOrderID and C.OrderItemId = P.OrderItemId and C.PostedDate = P.PostedDate left join Tax Z on C.AmazonOrderID=Z.AmazonOrderID and C.OrderItemId = Z.OrderItemId and C.PostedDate = Z.PostedDate ; create or replace table VAHDAM_DB.MAPLEMONK.vahdam_IN_refund_event_list as With Charge as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,Currency ,sum(ifnull(Tax,0)) Tax ,sum(ifnull(Principal,0)) Principal ,sum(ifnull(ShippingTax,0)) ShippingTax ,sum(ifnull(ShippingCharge,0)) ShippingCharge ,sum(ifnull(GiftWrap,0)) GiftWrap ,sum(ifnull(GiftWrapTax,0)) GiftWrapTax ,sum(ifnull(TCS_IGST,0)) \"TCS-IGST\" ,sum(ifnull(TCS_CGST,0)) \"TCS-CGST\" ,sum(ifnull(TCS_SGST,0)) \"TCS-SGST\" from (select * from (select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, C.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(C.value:ChargeType) = \'tax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Tax, case when lower(C.value:ChargeType) = \'principal\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Principal, case when lower(C.value:ChargeType) = \'shippingtax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingTax, case when lower(C.value:ChargeType) = \'shippingcharge\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingCharge, case when lower(C.value:ChargeType) = \'giftwrap\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrap, case when lower(C.value:ChargeType) = \'giftwraptax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrapTax, case when lower(C.value:ChargeType) = \'tcs-igst\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as TCS_IGST, case when lower(C.value:ChargeType) = \'tcs-cgst\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as TCS_CGST, case when lower(C.value:ChargeType) = \'tcs-sgst\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as TCS_SGST, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B, lateral flatten(input => B.value:ItemChargeAdjustmentList)C where lower(A.value:MarketplaceName) = \'amazon.in\') where rw = 1 ) group by 1,2,3,4,5,6,7) , Fee as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,Currency ,sum(ifnull(Commission,0)) Commission ,sum(ifnull(RefundCommission,0)) RefundCommission ,sum(ifnull(GiftwrapChargeback,0)) GiftwrapChargeback ,sum(ifnull(ShippingChargeback,0)) ShippingChargeback ,sum(ifnull(FixedClosingFee,0)) FixedClosingFee from (select * from (Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, D.value:\"FeeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:FeeType) = \'commission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as Commission, case when lower(D.value:FeeType) = \'refundcommission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as RefundCommission, case when lower(D.value:FeeType) = \'giftwrapchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as GiftwrapChargeback, case when lower(D.value:FeeType) = \'shippingchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as ShippingChargeback, case when lower(D.value:FeeType) = \'fixedclosingfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as FixedClosingFee, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input =>B.value:ItemFeeAdjustmentList)D where lower(A.value:MarketplaceName) = \'amazon.in\') where rw = 1) group by 1,2,3,4,5,6,7 ) , Quantity as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,sum(ifnull(Quantity,0)) as Quantity from (select * from (Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, B.value:QuantityShipped as Quantity, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B where lower(A.value:MarketplaceName) = \'amazon.in\') where rw = 1) group by 1,2,3,4,5,6 ) , Promotion as ( select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,sum(ifnull(Refund_Promotion,0)) as Refund_Promotion from ( select * from ( Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, P.value:\"PromotionAmount\":\"CurrencyAmount\" as Refund_Promotion, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input => B.value:PromotionAdjustmentList)P where lower(A.value:MarketplaceName) = \'amazon.in\' ) where rw = 1 ) group by 1,2,3,4,5,6 ) , TCM as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,TaxCollectionModel from (select * from ( Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, E.value:TaxCollectionModel as TaxCollectionModel, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input => B.value:ItemTaxWithheldList)E, lateral flatten(input =>E.value:TaxesWithheld)F where lower(A.value:MarketplaceName) = \'amazon.in\') where rw = 1) ) select replace(coalesce(C.AmazonOrderID,F.AmazonOrderID,Q.AmazonOrderID,T.AmazonOrderID,P.AmazonOrderID),\'\"\"\',\'\') as AmazonOrderID ,replace(coalesce(C.MarketplaceName,F.MarketplaceName,Q.MarketplaceName,T.MarketplaceName,P.MarketplaceName),\'\"\"\',\'\') as MarketplaceName ,replace(coalesce(C.PostedDate,F.PostedDate,Q.PostedDate,T.PostedDate,P.PostedDate),\'\"\"\',\'\') as PostedDate ,replace(coalesce(C.SellerOrderId,F.SellerOrderId,Q.SellerOrderId,T.SellerOrderId,P.SellerOrderId),\'\"\"\',\'\') as SellerOrderId ,replace(coalesce(C.SellerSKU,F.SellerSKU,Q.SellerSKU,T.SellerSKU,P.SellerSKU),\'\"\"\',\'\') SellerSKU ,replace(coalesce(C.OrderAdjustmentItemId,F.OrderAdjustmentItemId,Q.OrderAdjustmentItemId,T.OrderAdjustmentItemId,P.OrderAdjustmentItemId),\'\"\"\',\'\') OrderAdjustmentItemId ,replace(coalesce(C.Currency,F.Currency),\'\"\"\',\'\') Currency ,Q.Quantity ,ifnull(P.Refund_Promotion,0) as Refund_Promotion ,C.Tax ,C.Principal ,C.ShippingTax ,C.ShippingCharge ,C.GiftWrap ,C.GiftWrapTax ,C.\"TCS-IGST\" ,C.\"TCS-CGST\" ,C.\"TCS-SGST\" ,F.Commission ,F.RefundCommission ,F.GiftwrapChargeback ,F.ShippingChargeback ,F.FixedClosingFee from Charge C left join Fee F on C.AmazonOrderID=F.AmazonOrderID and C.OrderAdjustmentItemId = F.OrderAdjustmentItemId and C.PostedDate = F.PostedDate left join Quantity Q on C.AmazonOrderID=Q.AmazonOrderID and C.OrderAdjustmentItemId = Q.OrderAdjustmentItemId and C.PostedDate = Q.PostedDate left join TCM T on C.AmazonOrderID=T.AmazonOrderID and C.OrderAdjustmentItemId = T.OrderAdjustmentItemId and C.PostedDate = T.PostedDate left join Promotion P on C.AmazonOrderID=P.AmazonOrderID and C.OrderAdjustmentItemId = P.OrderAdjustmentItemId and C.PostedDate = P.PostedDate; create or replace table vahdam_db.maplemonk.vahdam_IN_adjustment_event_list as (select PostedDate ,ProductDescription ,SellerSKU ,Currency ,sum(ifnull(WAREHOUSE_DAMAGE_Quantity,0)) WAREHOUSE_DAMAGE_Quantity ,sum(ifnull(WAREHOUSE_DAMAGE_Total_Amount,0)) WAREHOUSE_DAMAGE_Total_Amount ,sum(ifnull(REVERSAL_REIMBURSEMENT_Quantity,0)) REVERSAL_REIMBURSEMENT_Quantity ,sum(ifnull(REVERSAL_REIMBURSEMENT_Total_Amount,0)) REVERSAL_REIMBURSEMENT_Total_Amount ,sum(ifnull(FREE_REPLACEMENT_REFUND_ITEMS_Quantity,0)) FREE_REPLACEMENT_REFUND_ITEMS_Quantity ,sum(ifnull(FREE_REPLACEMENT_REFUND_ITEMS_Total_Amount,0)) FREE_REPLACEMENT_REFUND_ITEMS_Total_Amount from (select * from (select replace(A.value:PostedDate,\'\"\',\'\') as PostedDate ,replace(B.value:ProductDescription,\'\"\"\',\'\') as ProductDescription ,replace(B.value:SellerSKU,\'\"\',\'\') as SellerSKU ,replace(B.value:\"TotalAmount\":\"CurrencyCode\",\'\"\',\'\') as Currency ,case when A.value:AdjustmentType = \'WAREHOUSE_DAMAGE\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as WAREHOUSE_DAMAGE_Quantity ,case when A.value:AdjustmentType = \'WAREHOUSE_DAMAGE\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as WAREHOUSE_DAMAGE_Total_Amount ,case when A.value:AdjustmentType = \'REVERSAL_REIMBURSEMENT\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as REVERSAL_REIMBURSEMENT_Quantity ,case when A.value:AdjustmentType = \'REVERSAL_REIMBURSEMENT\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as REVERSAL_REIMBURSEMENT_Total_Amount ,case when A.value:AdjustmentType = \'FREE_REPLACEMENT_REFUND_ITEMS\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as FREE_REPLACEMENT_REFUND_ITEMS_Quantity ,case when A.value:AdjustmentType = \'FREE_REPLACEMENT_REFUND_ITEMS\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as FREE_REPLACEMENT_REFUND_ITEMS_Total_Amount ,rank() over (partition by PostedDate, ProductDescription, SellerSKU, Currency order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents,lateral flatten(input =>ADJUSTMENTEVENTLIST)A, lateral flatten (input => A.value:AdjustmentItemList)B ) where rw = 1) group by 1,2,3,4 ); create or replace table vahdam_db.maplemonk.vahdam_in_removal_disposal_fees as (select date,replace(order_id,\'\"\"\',\'\') Order_ID,replace(fee_type,\'\"\"\',\'\') Fee_type,replace(CURRENCY,\'\"\"\',\'\') CURRENCY, sum(amount) Amount from ( select *, dense_rank() over (partition by order_id order by date) rw from (select date,servicefeeeventlist,A.value:AmazonOrderId Order_id,B.value:FeeType Fee_type,B.value:\"FeeAmount\":\"CurrencyCode\" CURRENCY, sum(B.value:\"FeeAmount\":\"CurrencyAmount\") Amount from (select date, servicefeeeventlist from (select servicefeeeventlist,_airbyte_emitted_at::date Date, row_number() over (partition by servicefeeeventlist order by _airbyte_emitted_at::date asc) rw from vahdam_db.maplemonk.casp_in_listfinancialevents) where rw = 1 and lower(servicefeeeventlist) like any (\'%disposal%\',\'%removal%\') ) ,LATERAL FLATTEN (INPUT => servicefeeeventlist)A ,LATERAL FLATTEN (INPUT => A.VALUE:FeeList) B group by 1,2,3,4,5)) where rw = 1 and lower(fee_type) like any (\'%disposal%\',\'%removal%\') group by 1,2,3,4) ; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_IN_MONTHLY_STORAGE_FEES_PR AS WITH STORAGECOST AS (WITH DATES AS ( with RECURSIVE date_range AS ( SELECT TO_DATE(\'2023-04-01\') AS date_value UNION ALL SELECT date_value + INTERVAL \'1 day\' FROM date_range WHERE date_value + INTERVAL \'1 day\' <= \'2024-03-31\' ) SELECT date_value FROM date_range), STORAGE AS (select date,replace(fee_type,\'\"\"\',\'\') Fee_type,replace(CURRENCY,\'\"\"\',\'\') CURRENCY, sum(amount) Amount from ( select *, dense_rank() over (partition by order_id,date order by date) rw from (select date,servicefeeeventlist,A.value:AmazonOrderId Order_id,B.value:FeeType Fee_type,B.value:\"FeeAmount\":\"CurrencyCode\" CURRENCY, sum(B.value:\"FeeAmount\":\"CurrencyAmount\") Amount from (select date, servicefeeeventlist from (select servicefeeeventlist,_airbyte_emitted_at::date Date, row_number() over (partition by servicefeeeventlist order by _airbyte_emitted_at::date asc) rw from vahdam_db.maplemonk.casp_IN_listfinancialevents) where rw = 1 and lower(servicefeeeventlist) like \'%storage%\' ) ,LATERAL FLATTEN (INPUT => servicefeeeventlist)A ,LATERAL FLATTEN (INPUT => A.VALUE:FeeList) B group by 1,2,3,4,5)) where rw = 1 and lower(fee_type) like \'%storage%\' AND CURRENCY = \'INR\' group by 1,2,3) SELECT COALESCE(D.DATE_VALUE,S.DATE) AS DATE, IFNULL(S.FEE_TYPE,\'NA\') FEE_TYPE, IFNULL(S.CURRENCY,\'NA\') CURRENCY,SUM(IFNULL(S.AMOUNT,0)) AMOUNT FROM DATES D LEFT JOIN STORAGE S ON D.DATE_VALUE=S.DATE GROUP BY 1,2,3) SELECT MONTH,YEAR,SUM(IFNULL(STFS_CURRENT_MONTH,0)) STFS_CURRENT_MONTH, SUM(IFNULL(LTFS_CURRENT_MONTH,0)) LTFS_CURRENT_MONTH FROM (SELECT MONTH(DATE) AS MONTH, YEAR(DATE) AS YEAR, CASE WHEN FEE_TYPE = \'FBAStorageFee\' THEN SUM(IFNULL(AMOUNT,0)) ELSE 0 END AS STFS_CURRENT_MONTH ,CASE WHEN FEE_TYPE = \'FBALongTermStorageFee\' THEN SUM(IFNULL(AMOUNT,0)) ELSE 0 END AS LTFS_CURRENT_MONTH FROM STORAGECOST GROUP BY FEE_TYPE,1,2 ORDER BY 2,1) GROUP BY 1,2 ORDER BY 2,1;",
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
                        