{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT * FROM maplemonk.test_v_get_clients; SELECT c.Id, c.City, c.Email, c.Notes, c.State, c.Action, c.Active, c.Gender, c.Status, c.Country, c.LastName, c.PhotoUrl, c.RedAlert, c.UniqueId, c.BirthDate, c.FirstName, c.HomePhone, c.IsCompany, JSON_VALUE(c.Liability, \'$.ReleasedBy\') AS Liability_ReleasedBy, JSON_VALUE(c.Liability, \'$.AgreementDate\') AS Liability_AgreementDate, JSON_VALUE(c.Liability, \'$.IsReleased\') AS Liability_IsReleased, JSON_VALUE(rep, \'$.FirstName\') AS SalesRep_FirstName, JSON_VALUE(rep, \'$.LastName\') AS SalesRep_LastName, JSON_VALUE(rep, \'$.Id\') AS SalesRep_Id, JSON_VALUE(rep, \'$.SalesRepNumber\') AS SalesRep_Number, c.WorkPhone, c.IsProspect, c.MiddleName, c.PostalCode, c.ReferredBy, c.MobilePhone, c.YellowAlert, c.AddressLine1, c.AddressLine2, c.CreationDate, c.HomeLocation, c.LockerNumber, ProspectStage, WorkExtension ,AccountBalance, FirstClassDate,MembershipIcon, MobileProvider, SuspensionInfo, JSON_VALUE(c.ClientCreditCard, \'$.CardNumber\') AS CardNumber, JSON_VALUE(c.ClientCreditCard, \'$.CardType\') AS CardType, JSON_VALUE(c.ClientCreditCard, \'$.ExpMonth\') AS ExpMonth, JSON_VALUE(c.ClientCreditCard, \'$.ExpYear\') AS ExpYear, JSON_VALUE(c.ClientCreditCard, \'$.LastFour\') AS LastFour, JSON_VALUE(c.ClientCreditCard, \'$.PostalCode\') AS PostalCode, LastFormulaNotes, LiabilityRelease, SendAccountTexts, SendAccountEmails, SendScheduleTexts, CustomClientFields, SendScheduleEmails, FirstAppointmentDate, LastModifiedDateTime, SendPromotionalTexts, SendPromotionalEmails, EmergencyContactInfoName, EmergencyContactInfoEmail, EmergencyContactInfoPhone, AppointmentGenderPreference, EmergencyContactInfoRelationship, _airbyte_ab_id ,_airbyte_emitted_at, _airbyte_normalized_at, _airbyte_test_v_get_clients_hashid, JSON_VALUE(rel, \'$.RelationshipName\') AS RelationshipName, JSON_VALUE(rel, \'$.RelatedClientId\') AS RelatedClientId, JSON_VALUE(rel, \'$.Relationship.Id\') AS Relationship_Id, JSON_VALUE(rel, \'$.Relationship.RelationshipName1\') AS RelationshipName1, JSON_VALUE(rel, \'$.Relationship.RelationshipName2\') AS RelationshipName2 FROM `maplemonk.test_v_get_clients` c LEFT JOIN UNNEST(c.SalesReps) AS rep LEFT JOIN UNNEST(c.ClientRelationships) AS rel",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            