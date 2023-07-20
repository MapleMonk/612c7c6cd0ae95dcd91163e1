{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.vahdam_UK_shipment_event_list as With Tax as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(MarketplaceFacilitatorTaxOther,0)) MarketplaceFacilitatorTaxOther, sum(ifnull(MarketplaceFacilitatorVATShipping,0)) MarketplaceFacilitatorVATShipping, sum(ifnull(MarketplaceFacilitatorVATPrincipal,0)) MarketplaceFacilitatorVATPrincipal from (select * from (Select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, D.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:ChargeType) = \'marketplacefacilitatortax-other\' then D.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end MarketplaceFacilitatorTaxOther, case when lower(D.value:ChargeType) = \'marketplacefacilitatorvat-shipping\' then D.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end MarketplaceFacilitatorVATShipping, case when lower(D.value:ChargeType) = \'marketplacefacilitatorvat-principal\' then D.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end MarketplaceFacilitatorVATPrincipal, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemTaxWithheldList)C ,lateral flatten(input =>C.value:TaxesWithheld)D ) where rw = 1) group by 1,2,3,4,5,6,7 ), Promotion as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(Promotion,0)) Promotion from (select * from ( Select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, C.value:\"PromotionAmount\":\"CurrencyCode\" as Currency, ifnull(C.value:\"PromotionAmount\":\"CurrencyAmount\",0) Promotion, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:PromotionList)C ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Charge as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(Principal,0)) Principal, sum(ifnull(Tax,0)) Tax, sum(ifnull(GiftWrap,0)) GiftWrap, sum(ifnull(GiftWrapTax,0)) GiftWrapTax, sum(ifnull(ShippingCharge,0)) ShippingCharge, sum(ifnull(ShippingTax,0)) ShippingTax from (select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, C.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(C.value:ChargeType) = \'principal\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Principal, case when lower(C.value:ChargeType) = \'tax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Tax, case when lower(C.value:ChargeType) = \'giftwrap\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrap, case when lower(C.value:ChargeType) = \'giftwraptax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrapTax, case when lower(C.value:ChargeType) = \'shippingcharge\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingCharge, case when lower(C.value:ChargeType) = \'shippingtax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingTax, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemChargeList)C ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Fee as (select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, Currency, sum(ifnull(FBAPerUnitFulfillmentFee,0)) FBAPerUnitFulfillmentFee, sum(ifnull(Commission,0)) Commission, sum(ifnull(FixedclosingFee,0)) FixedclosingFee, sum(ifnull(GiftWrapChargeback,0)) GiftWrapChargeback, sum(ifnull(ShippingChargeback,0)) ShippingChargeback, sum(ifnull(VariableClosingFee,0)) VariableClosingFee from (select * from ( select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, D.value:\"FeeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:FeeType) = \'fbaperunitfulfillmentfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as FBAPerUnitFulfillmentFee, case when lower(D.value:FeeType) = \'commission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as Commission, case when lower(D.value:FeeType) = \'fixedclosingfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as FixedclosingFee, case when lower(D.value:FeeType) = \'giftwrapchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as GiftWrapChargeback, case when lower(D.value:FeeType) = \'shippingchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as ShippingChargeback, case when lower(D.value:FeeType) = \'variableclosingfee\' then D.value:\"FeeAmount\":\"CurrencyAmount\" end as VariableClosingFee, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemFeeList)D ) where rw = 1 ) group by 1,2,3,4,5,6,7 ), Quantity as ( select AmazonOrderId, MarketplaceName, PostedDate, SellerOrderId, OrderItemId, SellerSKU, sum(ifnull(Quantity,0)) Quantity from (select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, B.value:QuantityShipped as Quantity, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B) where rw = 1) group by 1,2,3,4,5,6 ) , TCM as (select AmazonOrderId ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderItemId ,SellerSKU ,TaxCollectionModel from ( select * from (select A.value:AmazonOrderId as AmazonOrderId, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderItemId as OrderItemId, B.value:SellerSKU as SellerSKU, E.value:TaxCollectionModel as TaxCollectionModel, rank() over (partition by A.value:AmazonOrderId, A.value:MarketplaceName, A.value:PostedDate, A.value:SellerOrderId, B.value:OrderItemId, B.value:SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents ,lateral flatten(input =>SHIPMENTEVENTLIST)A ,lateral flatten(input =>A.value:ShipmentItemList)B ,lateral flatten(input =>B.value:ItemTaxWithheldList)E) where rw = 1) ) select replace(coalesce(C.AmazonOrderId,F.AmazonOrderId,Q.AmazonOrderId,T.AmazonOrderId,P.AmazonOrderId,Z.AmazonOrderId),\'\"\"\',\'\') as AmazonOrderId ,replace(coalesce(C.MarketplaceName,F.MarketplaceName,Q.MarketplaceName,T.MarketplaceName,P.MarketplaceName,Z.MarketplaceName),\'\"\"\',\'\') as MarketplaceName ,replace(coalesce(C.PostedDate,F.PostedDate,Q.PostedDate,T.PostedDate,P.PostedDate,Z.PostedDate),\'\"\"\',\'\') as PostedDate ,replace(coalesce(C.SellerOrderId,F.SellerOrderId,Q.SellerOrderId,T.SellerOrderId,P.SellerOrderId,Z.SellerOrderId),\'\"\"\',\'\') as SellerOrderId ,replace(coalesce(C.OrderItemId,F.OrderItemId,Q.OrderItemId,T.OrderItemId,P.OrderItemId,Z.OrderItemId),\'\"\"\',\'\') as OrderItemId ,replace(coalesce(C.SellerSKU,F.SellerSKU,Q.SellerSKU,T.SellerSKU,P.SellerSKU,Z.SellerSKU),\'\"\"\',\'\') as SellerSKU ,replace(coalesce(C.Currency,F.Currency,P.Currency,Z.Currency),\'\"\"\',\'\') as Currency ,Q.Quantity ,C.Principal ,F.FBAPerUnitFulfillmentFee ,F.Commission ,ifnull(P.Promotion,0) Promotion ,C.Tax ,C.GiftWrap ,C.GiftWrapTax ,C.ShippingCharge ,C.ShippingTax ,ifnull(Z.MarketplaceFacilitatorTaxOther,0) MarketplaceFacilitatorTaxOther ,ifnull(Z.MarketplaceFacilitatorVATShipping,0) MarketplaceFacilitatorVATShipping ,ifnull(Z.MarketplaceFacilitatorVATPrincipal,0) MarketplaceFacilitatorVATPrincipal ,F.FixedclosingFee ,F.GiftWrapChargeback ,F.ShippingChargeback ,F.VariableClosingFee ,T.TaxCollectionModel from Charge C left join Fee F on C.AmazonOrderID=F.AmazonOrderID and C.OrderItemId = F.OrderItemId and C.PostedDate = F.PostedDate left join Quantity Q on C.AmazonOrderID=Q.AmazonOrderID and C.OrderItemId = Q.OrderItemId and C.PostedDate = Q.PostedDate left join TCM T on C.AmazonOrderID=T.AmazonOrderID and C.OrderItemId = T.OrderItemId and C.PostedDate = T.PostedDate left join Promotion P on C.AmazonOrderID=P.AmazonOrderID and C.OrderItemId = P.OrderItemId and C.PostedDate = P.PostedDate left join Tax Z on C.AmazonOrderID=Z.AmazonOrderID and C.OrderItemId = Z.OrderItemId and C.PostedDate = Z.PostedDate ; create or replace table VAHDAM_DB.MAPLEMONK.vahdam_UK_refund_event_list as With Charge as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,Currency ,sum(ifnull(Tax,0)) Tax ,sum(ifnull(Principal,0)) Principal ,sum(ifnull(ShippingTax,0)) ShippingTax ,sum(ifnull(ShippingCharge,0)) ShippingCharge ,sum(ifnull(GiftWrap,0)) GiftWrap ,sum(ifnull(GiftWrapTax,0)) GiftWrapTax ,sum(ifnull(ExportCharge,0)) ExportCharge ,sum(ifnull(ReturnShipping,0)) ReturnShipping ,sum(ifnull(GenericDeduction,0)) GenericDeduction ,sum(ifnull(Goodwill,0)) Goodwill from (select * from (select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, C.value:\"ChargeAmount\":\"CurrencyCode\" as Currency, case when lower(C.value:ChargeType) = \'tax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Tax, case when lower(C.value:ChargeType) = \'principal\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Principal, case when lower(C.value:ChargeType) = \'shippingtax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingTax, case when lower(C.value:ChargeType) = \'shippingcharge\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ShippingCharge, case when lower(C.value:ChargeType) = \'giftwrap\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrap, case when lower(C.value:ChargeType) = \'giftwraptax\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GiftWrapTax, case when lower(C.value:ChargeType) = \'exportcharge\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ExportCharge, case when lower(C.value:ChargeType) = \'ReturnShipping\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as ReturnShipping, case when lower(C.value:ChargeType) = \'GenericDeduction\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as GenericDeduction, case when lower(C.value:ChargeType) = \'Goodwill\' then C.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Goodwill, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B, lateral flatten(input => B.value:ItemChargeAdjustmentList)C) where rw = 1 ) group by 1,2,3,4,5,6,7) , Fee as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,Currency ,sum(ifnull(Commission,0)) Commission ,sum(ifnull(RefundCommission,0)) RefundCommission ,sum(ifnull(GiftwrapChargeback,0)) GiftwrapChargeback ,sum(ifnull(ShippingChargeback,0)) ShippingChargeback from (select * from (Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, D.value:\"FeeAmount\":\"CurrencyCode\" as Currency, case when lower(D.value:FeeType) = \'commission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as Commission, case when lower(D.value:FeeType) = \'refundcommission\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as RefundCommission, case when lower(D.value:FeeType) = \'giftwrapchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as GiftwrapChargeback, case when lower(D.value:FeeType) = \'shippingchargeback\' then D.value:\"FeeAmount\":\"CurrencyAmount\" else 0 end as ShippingChargeback, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input =>B.value:ItemFeeAdjustmentList)D) where rw = 1) where AmazonOrderID = \'303-0411507-7134734\' group by 1,2,3,4,5,6,7 ) , Quantity as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,sum(ifnull(Quantity,0)) as Quantity from (select * from (Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, B.value:QuantityShipped as Quantity, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B) where rw = 1) group by 1,2,3,4,5,6 ) , Promotion as ( select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,sum(ifnull(Refund_Promotion,0)) as Refund_Promotion from ( select * from ( Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, P.value:\"PromotionAmount\":\"CurrencyAmount\" as Refund_Promotion, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input => B.value:PromotionAdjustmentList)P ) where rw = 1 ) group by 1,2,3,4,5,6 ) , TCM as (select AmazonOrderID ,MarketplaceName ,PostedDate ,SellerOrderId ,OrderAdjustmentItemId ,SellerSKU ,sum(ifnull(Refund_MarketplaceFacilitatorTax_Other,0)) Refund_MarketplaceFacilitatorTax_Other ,sum(ifnull(Refund_MarketplaceFacilitatorVat_Shipping,0)) Refund_MarketplaceFacilitatorVat_Shipping ,sum(ifnull(Refund_MarketplaceFacilitatorVat_Principal,0)) Refund_MarketplaceFacilitatorVat_Principal from (select * from ( Select A.value:AmazonOrderId as AmazonOrderID, A.value:MarketplaceName as MarketplaceName, A.value:PostedDate as PostedDate, A.value:SellerOrderId as SellerOrderId, B.value:OrderAdjustmentItemId as OrderAdjustmentItemId, B.value:SellerSKU as SellerSKU, E.value:TaxCollectionModel as TaxCollectionModel, case when lower(F.value:\"ChargeType\") = \'marketplacefacilitatortax-other\' then F.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Refund_MarketplaceFacilitatorTax_Other, case when lower(F.value:\"ChargeType\") = \'marketplacefacilitatorvat-shipping\' then F.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Refund_MarketplaceFacilitatorVat_Shipping, case when lower(F.value:\"ChargeType\") = \'marketplacefacilitatorvat-principal\' then F.value:\"ChargeAmount\":\"CurrencyAmount\" else 0 end as Refund_MarketplaceFacilitatorVat_Principal, rank() over (partition by AmazonOrderID, MarketplaceName, PostedDate, SellerOrderId, OrderAdjustmentItemId, SellerSKU order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents,lateral flatten(input => refundeventlist) A,lateral flatten(input =>A.value:ShipmentItemAdjustmentList)B,lateral flatten(input => B.value:ItemTaxWithheldList)E, lateral flatten(input =>E.value:TaxesWithheld)F) where rw = 1) group by 1,2,3,4,5,6 ) select replace(coalesce(C.AmazonOrderID,F.AmazonOrderID,Q.AmazonOrderID,T.AmazonOrderID,P.AmazonOrderID),\'\"\"\',\'\') as AmazonOrderID ,replace(coalesce(C.MarketplaceName,F.MarketplaceName,Q.MarketplaceName,T.MarketplaceName,P.MarketplaceName),\'\"\"\',\'\') as MarketplaceName ,replace(coalesce(C.PostedDate,F.PostedDate,Q.PostedDate,T.PostedDate,P.PostedDate),\'\"\"\',\'\') as PostedDate ,replace(coalesce(C.SellerOrderId,F.SellerOrderId,Q.SellerOrderId,T.SellerOrderId,P.SellerOrderId),\'\"\"\',\'\') as SellerOrderId ,replace(coalesce(C.SellerSKU,F.SellerSKU,Q.SellerSKU,T.SellerSKU,P.SellerSKU),\'\"\"\',\'\') SellerSKU ,replace(coalesce(C.OrderAdjustmentItemId,F.OrderAdjustmentItemId,Q.OrderAdjustmentItemId,T.OrderAdjustmentItemId,P.OrderAdjustmentItemId),\'\"\"\',\'\') OrderAdjustmentItemId ,Q.Quantity ,C.Tax ,C.Principal ,ifnull(P.Refund_Promotion,0) as Refund_Promotion ,C.ShippingTax ,C.ShippingCharge ,C.GiftWrap ,C.GiftWrapTax ,C.ExportCharge ,C.ReturnShipping ,C.GenericDeduction ,C.Goodwill ,F.Commission ,F.RefundCommission ,F.GiftwrapChargeback ,F.ShippingChargeback ,ifnull(T.Refund_MarketplaceFacilitatorTax_Other,0) as Refund_MarketplaceFacilitatorTax_Other ,ifnull(T.Refund_MarketplaceFacilitatorVat_Shipping,0) as Refund_MarketplaceFacilitatorVat_Shipping ,ifnull(T.Refund_MarketplaceFacilitatorVat_Principal,0)as Refund_MarketplaceFacilitatorVat_Principal from Charge C left join Fee F on C.AmazonOrderID=F.AmazonOrderID and C.OrderAdjustmentItemId = F.OrderAdjustmentItemId and C.PostedDate=F.PostedDate left join Quantity Q on C.AmazonOrderID=Q.AmazonOrderID and C.OrderAdjustmentItemId = Q.OrderAdjustmentItemId and C.PostedDate=Q.PostedDate left join TCM T on C.AmazonOrderID=T.AmazonOrderID and C.OrderAdjustmentItemId = T.OrderAdjustmentItemId and C.PostedDate=T.PostedDate left join Promotion P on C.AmazonOrderID=P.AmazonOrderID and C.OrderAdjustmentItemId = P.OrderAdjustmentItemId and C.PostedDate=P.PostedDate; create or replace table vahdam_db.maplemonk.vahdam_uk_adjustment_event_list as (select PostedDate ,ProductDescription ,SellerSKU ,Currency ,sum(ifnull(Compensated_Clawback_Quantity,0)) Compensated_Clawback_Quantity ,sum(ifnull(Compensated_Clawback_Total_Amount,0)) Compensated_Clawback_Total_Amount ,sum(ifnull(WAREHOUSE_DAMAGE_Quantity,0)) WAREHOUSE_DAMAGE_Quantity ,sum(ifnull(WAREHOUSE_DAMAGE_Total_Amount,0)) WAREHOUSE_DAMAGE_Total_Amount ,sum(ifnull(REVERSAL_REIMBURSEMENT_Quantity,0)) REVERSAL_REIMBURSEMENT_Quantity ,sum(ifnull(REVERSAL_REIMBURSEMENT_Total_Amount,0)) REVERSAL_REIMBURSEMENT_Total_Amount ,sum(ifnull(WAREHOUSE_LOST_MANUAL_Quantity,0)) WAREHOUSE_LOST_MANUAL_Quantity ,sum(ifnull(WAREHOUSE_LOST_MANUAL_Total_Amount,0)) WAREHOUSE_LOST_MANUAL_Total_Amount ,sum(ifnull(WAREHOUSE_DAMAGE_EXCEPTION_Quantity,0)) WAREHOUSE_DAMAGE_EXCEPTION_Quantity ,sum(ifnull(WAREHOUSE_DAMAGE_EXCEPTION_Total_Amount,0)) WAREHOUSE_DAMAGE_EXCEPTION_Total_Amount from (select * from (select replace(A.value:PostedDate,\'\"\',\'\') as PostedDate ,replace(B.value:ProductDescription,\'\"\"\',\'\') as ProductDescription ,replace(B.value:SellerSKU,\'\"\',\'\') as SellerSKU ,replace(B.value:\"TotalAmount\":\"CurrencyCode\",\'\"\',\'\') as Currency ,case when A.value:AdjustmentType = \'COMPENSATED_CLAWBACK\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as Compensated_Clawback_Quantity ,case when A.value:AdjustmentType = \'COMPENSATED_CLAWBACK\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as Compensated_Clawback_Total_Amount ,case when A.value:AdjustmentType = \'WAREHOUSE_DAMAGE\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as WAREHOUSE_DAMAGE_Quantity ,case when A.value:AdjustmentType = \'WAREHOUSE_DAMAGE\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as WAREHOUSE_DAMAGE_Total_Amount ,case when A.value:AdjustmentType = \'REVERSAL_REIMBURSEMENT\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as REVERSAL_REIMBURSEMENT_Quantity ,case when A.value:AdjustmentType = \'REVERSAL_REIMBURSEMENT\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as REVERSAL_REIMBURSEMENT_Total_Amount ,case when A.value:AdjustmentType = \'WAREHOUSE_LOST_MANUAL\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as WAREHOUSE_LOST_MANUAL_Quantity ,case when A.value:AdjustmentType = \'WAREHOUSE_LOST_MANUAL\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as WAREHOUSE_LOST_MANUAL_Total_Amount ,case when A.value:AdjustmentType = \'WAREHOUSE_DAMAGE_EXCEPTION\' then replace(B.value:Quantity,\'\"\',\'\') else 0 end as WAREHOUSE_DAMAGE_EXCEPTION_Quantity ,case when A.value:AdjustmentType = \'WAREHOUSE_DAMAGE_EXCEPTION\' then replace(B.value:\"TotalAmount\":\"CurrencyAmount\",\'\"\',\'\') else 0 end as WAREHOUSE_DAMAGE_EXCEPTION_Total_Amount ,rank() over (partition by PostedDate, ProductDescription, SellerSKU, Currency order by _airbyte_emitted_at desc) rw from vahdam_db.maplemonk.casp_uk_listfinancialevents,lateral flatten(input =>ADJUSTMENTEVENTLIST)A, lateral flatten (input => A.value:AdjustmentItemList)B ) where rw = 1) group by 1,2,3,4 );",
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
                        