{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table FieldAssist_Outlet_Employee_Date_XYXX_Temp as with sales as ( select time ,f.EMPOYEEERPID ,f.employeename ,f.employeetype ,f.EMPLOYEEDESIGNATION ,f.outleterpid as OutletERPId ,f.isnewoutlet as FA_isNewOutlet ,A.Value:\"DistributorType\" as DistributorType ,replace(outlet:\"AttributeText1\",\'\"\',\'\') as OutletAtttributeText1 ,replace(outlet:\"OutletChannelName\",\'\"\',\'\') as OutletChannelName ,replace(A.Value:\"ProductERPId\",\'\"\',\'\') as ProductErpId ,replace(A.Value:\"ProductName\",\'\"\',\'\') as ProductName ,replace(A.Value:\"Variant\",\'\"\',\'\') as ProductVariant ,f.visitid ,f.productive ,istelephonic ,f.callendtime ,f.callstarttime ,f.NOSALESREASON ,A.Value:\"Price\"::float as PricePerItem ,A.Value:\"Quantity\"::float as QuantityPerItem ,A.Value:\"Discount_Product\"::float as Discount_Product ,A.Value:\"SchemeCashDiscount\"::float as SchemeCashDiscount ,(ifnull(A.Value:\"Quantity\"::float,0)*ifnull(A.Value:\"Price\"::float,0)) as GrossValuePerItem ,(ifnull(A.Value:\"Quantity\"::float,0)*ifnull(A.Value:\"Price\"::float,0))-(ifnull(Discount_Product,0)) as NetSalesPerItem from fieldassist_detailed_visit f, LATERAL FLATTEN (INPUT => sales, outer=>true)A ) , OutletStartDate as ( select outleterpid, min(time) as FirstSaleDate from fieldassist_detailed_visit where grossvalue >0 group by outleterpid ) select S.time::date as Date ,S.EMPOYEEERPID ,S.employeename ,S.employeetype ,S.EMPLOYEEDESIGNATION ,S.DistributorType ,fo.beaterpid ,fo.beatname as BeatName ,fo.distributorerpid as DistributorERPID ,fo.DISTRIBUTORNAME as DistributorName ,S.OutletERPId ,fo.SHOPNAME as OutletName ,fo.CREATEDAT as CreatedAt ,fo.PINCODE as OutletPincode ,fo.SEGMENTATION as OutletSegmentation ,fo.OWNERNAME as OutletOwnerName ,fo.TERRITORY as Territory ,fo.ZONE as Zone ,fo.ISBLOCKED as IsBlocked ,fo.SHOPTYPE as OutletType ,fo.CONTACT as OutletContact ,fo.Email as OutletEmail ,fo.SHOPID as OutletID ,fo.REGION as OutletRegion ,fo.Market as OutletMarket ,fo.STATE as OutletState ,fo.CITY as OutletCity ,fo.ADDRESS as Address ,S.OutletAtttributeText1 ,S.OutletChannelName ,S.ProductErpId ,S.ProductName ,S.ProductVariant ,S.Productive ,S.visitid ,S.istelephonic ,datediff(seconds,S.callstarttime,S.callendtime) as TotalCallTime ,ifnull(S.PricePerItem,0) as PricePerItem ,ifnull(S.QuantityPerItem,0) as QuantityPerItem ,ifnull(S.Discount_Product,0) as DISCOUNT_PRODUCT ,ifnull(S.SchemeCashDiscount,0) as SCHEMECASHDISCOUNT ,ifnull(S.QuantityPerItem,0)*ifnull(S.PricePerItem,0) as GrossValuePerItem ,(ifnull(S.QuantityPerItem,0)*ifnull(S.PricePerItem,0))-ifnull(S.Discount_Product,0) as NetSalesPerItem ,SD.FirstSaleDate::date as OutletFirstSaleDate ,case when SD.FirstSaleDate::date = S.time::date then \'New\' when SD.FirstSaleDate::date < S.time::date then \'Repeat\' when SD.FirstSaleDate::date < S.time::date then \'Yet to make a sale\' end as OutletStatus_MM ,case when lower(OutletStatus_MM)=\'new\' then 1 else 0 end as NewOutlet_Flag ,case when month(OutletFirstSaleDate)=month(S.time::date) and year(OutletFirstSaleDate)=year(S.time::date) then 1 else 0 end as NewOutlet_Month_Flag from SALES S left join fieldassist_outlets fo on fo.outleterpid=S.OutletERPId left join OutletStartDate SD on S.OutletERPId=SD.outleterpid; create or replace table FieldAssist_Outlet_Employee_Date_XYXX_temp1 as select AO.ACTIVE_OUTLETS as ACTIVE_OUTLETS_BeatEmployeeDist,OEDT.* from FieldAssist_Outlet_Employee_Date_XYXX_Temp OEDT left join (SELECT X.DATE ,X.TERRITORY ,X.ZONE ,X.EMPOYEEERPID ,X.DISTRIBUTORERPID ,X.BEATERPID ,COUNT(DISTINCT Y.OUTLETERPID) ACTIVE_OUTLETS FROM( SELECT DISTINCT date, TERRITORY, ZONE, EMPOYEEERPID, DISTRIBUTORERPID, BEATERPID FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp ) X LEFT JOIN (SELECT DATE, OUTLETERPID, GROSSVALUEPERITEM, TERRITORY, ZONE, EMPOYEEERPID, DISTRIBUTORERPID, BEATERPID FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp) Y ON X.DATE>Y.DATE AND Y.GROSSVALUEPERITEM>0 AND X.TERRITORY=Y.TERRITORY AND X.ZONE=Y.ZONE AND X.EMPOYEEERPID=Y.EMPOYEEERPID AND X.DISTRIBUTORERPID=Y.DISTRIBUTORERPID AND X.BEATERPID=Y.BEATERPID GROUP BY X.DATE ,X.TERRITORY ,X.ZONE ,X.EMPOYEEERPID ,X.DISTRIBUTORERPID ,X.BEATERPID) AO on AO.Date= OEDT.Date and AO.TERRITORY=OEDT.TERRITORY and AO.ZONE=OEDT.ZONE and AO.EMPOYEEERPID=OEDT.EMPOYEEERPID and AO.DISTRIBUTORERPID = OEDT.DISTRIBUTORERPID and AO.BEATERPID = OEDT.BEATERPID; create or replace table FieldAssist_Outlet_Employee_Date_XYXX_temp2 as select AO.ACTIVE_OUTLETS as ACTIVE_OUTLETS_Employee,OEDT1.* from FieldAssist_Outlet_Employee_Date_XYXX_Temp1 OEDT1 left join (SELECT X.DATE ,X.TERRITORY ,X.ZONE ,X.EMPOYEEERPID ,COUNT(DISTINCT Y.OUTLETERPID) ACTIVE_OUTLETS FROM( SELECT DISTINCT date, TERRITORY, ZONE, EMPOYEEERPID FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp1 ) X LEFT JOIN (SELECT DATE, OUTLETERPID, GROSSVALUEPERITEM, TERRITORY, ZONE, EMPOYEEERPID FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp1) Y ON X.DATE>Y.DATE AND Y.GROSSVALUEPERITEM>0 AND X.TERRITORY=Y.TERRITORY AND X.ZONE=Y.ZONE AND X.EMPOYEEERPID=Y.EMPOYEEERPID GROUP BY X.DATE ,X.TERRITORY ,X.ZONE ,X.EMPOYEEERPID) AO on AO.Date= OEDT1.Date and AO.TERRITORY=OEDT1.TERRITORY and AO.ZONE=OEDT1.ZONE and AO.EMPOYEEERPID=OEDT1.EMPOYEEERPID; create or replace table FieldAssist_Outlet_Employee_Date_XYXX_temp3 as select AO.ACTIVE_OUTLETS as ACTIVE_OUTLETS_Territory,OEDT2.* from FieldAssist_Outlet_Employee_Date_XYXX_Temp2 OEDT2 left join (SELECT X.DATE ,X.TERRITORY ,X.ZONE ,COUNT(DISTINCT Y.OUTLETERPID) ACTIVE_OUTLETS FROM( SELECT DISTINCT date, TERRITORY, ZONE FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp2 ) X LEFT JOIN (SELECT DATE, OUTLETERPID, GROSSVALUEPERITEM, TERRITORY, ZONE FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp2) Y ON X.DATE>Y.DATE AND Y.GROSSVALUEPERITEM>0 AND X.TERRITORY=Y.TERRITORY AND X.ZONE=Y.ZONE GROUP BY X.DATE ,X.TERRITORY ,X.ZONE ) AO on AO.Date= OEDT2.Date and AO.TERRITORY=OEDT2.TERRITORY and AO.ZONE=OEDT2.ZONE; create or replace table FieldAssist_Outlet_Employee_Date_XYXX as select AO.ACTIVE_OUTLETS as ACTIVE_OUTLETS_Zone,OEDT3.* from FieldAssist_Outlet_Employee_Date_XYXX_Temp3 OEDT3 left join (SELECT X.DATE ,X.ZONE ,COUNT(DISTINCT Y.OUTLETERPID) ACTIVE_OUTLETS FROM( SELECT DISTINCT date, ZONE FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp3 ) X LEFT JOIN (SELECT DATE, OUTLETERPID, GROSSVALUEPERITEM, ZONE FROM maplemonk.fieldassist_outlet_employee_date_xyxx_temp2) Y ON X.DATE>Y.DATE AND Y.GROSSVALUEPERITEM>0 AND X.ZONE=Y.ZONE GROUP BY X.DATE ,X.ZONE ) AO on AO.Date= OEDT3.Date and AO.ZONE=OEDT3.ZONE; drop table fieldassist_outlet_employee_date_xyxx_temp; drop table fieldassist_outlet_employee_date_xyxx_temp2; drop table fieldassist_outlet_employee_date_xyxx_temp3;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        