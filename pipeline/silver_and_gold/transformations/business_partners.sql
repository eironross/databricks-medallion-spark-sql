/*
    Description:
        - Create a silver table of the business partners table
*/

CREATE MATERIALIZED VIEW business_partners 
COMMENT "Cleaned and validated business partners records joined with address info"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  TRIM(b.PARTNERID) AS partner_id
  ,TRIM(b.COMPANYNAME) AS company_name
  ,TRIM(b.LEGALFORM) AS legal_form
  ,TRIM(a.COUNTRY) AS country_name
  ,TRIM(a.REGION) AS region_name
  ,CAST(b.PARTNERROLE AS INT) AS partner_role
  ,TRIM(b.EMAILADDRESS) AS email_address
  ,TRIM(b.PHONENUMBER) AS phone_number
  ,TRIM(b.FAXNUMBER) AS fax_number
  ,TRIM(b.WEBADDRESS) AS web_address
FROM db_bike.01_bronze.bronze_business_partner b
INNER JOIN db_bike.01_bronze.bronze_addresses a ON b.ADDRESSID = a.ADDRESSID
WHERE 
   TRIM(b.PARTNERID) IS NOT NULL 
   OR  TRIM(b.PARTNERID) = "" -- drop if id is null