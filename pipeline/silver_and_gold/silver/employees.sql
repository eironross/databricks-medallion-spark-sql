/*
    Description:
        - Create a silver table of the employees table
*/

CREATE MATERIALIZED VIEW employees 
COMMENT "Cleaned and validated employee records joined with address info"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
  TRIM(b.EMPLOYEEID) AS employee_id
  ,TRIM(b.NAME_FIRST) AS first_name
  ,TRIM(b.NAME_MIDDLE) AS middle_name
  ,TRIM(b.NAME_LAST) AS last_name
  ,CASE
      TRIM(b.SEX)
      WHEN 'M' THEN 'Male'
      WHEN 'F' THEN 'Female'
      ELSE 'Unknown'
    END AS gender
  ,CONCAT(SUBSTRING(TRIM(b.NAME_FIRST), 1, 3),".",TRIM(b.NAME_LAST)) AS login_name
  ,CONCAT(SUBSTRING(TRIM(b.NAME_FIRST), 1, 3),".",TRIM(b.NAME_LAST),"@fontaine.com.ph") AS email_address
  ,TRIM(a.COUNTRY) AS country_name
  ,TRIM(a.REGION) AS region_name
  ,CASE 
      WHEN CAST(SUBSTRING(TRIM(b.VALIDITY_ENDDATE), 1, 4) AS INT) > YEAR(CURRENT_DATE()) THEN 1
      ELSE 0
  END AS is_active
FROM db_bike.01_bronze.bronze_employees b
INNER JOIN db_bike.01_bronze.bronze_addresses a ON b.ADDRESSID = a.ADDRESSID
WHERE
    b.EMPLOYEEID IS NOT NULL
    OR b.NAME_FIRST IS NOT NULL
    AND CAST(SUBSTRING(TRIM(b.VALIDITY_ENDDATE), 1, 4) AS INT) > YEAR(CURRENT_DATE()); 


