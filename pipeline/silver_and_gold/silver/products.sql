/*
    Description:
        - Create a silver table of the products table
*/
CREATE MATERIALIZED VIEW products 
COMMENT "Cleaned and validated products, joined category text and texts"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
  TRIM(a.PRODUCTID) AS product_id
  ,TRIM(a.TYPECODE) AS type_code
  ,TRIM(a.SUPPLIER_PARTNERID) AS partner_id
  ,CAST(TRIM(a.PRICE) AS DECIMAL(10,2)) AS list_price
  ,TRIM(a.CURRENCY) AS currency
  ,TRIM(a.PRODCATEGORYID) AS product_category_id
  ,TRIM(c.SHORT_DESCR) AS product_category
  ,CASE
     WHEN TRIM(b.MEDIUM_DESCR) IS NULL THEN TRIM(b.SHORT_DESCR)
     ELSE TRIM(b.MEDIUM_DESCR)
  END AS product_description
  ,CAST(TRIM(a.WEIGHTMEASURE) AS DECIMAL(10,2)) AS weight_measure
  ,TRIM(a.WEIGHTUNIT) AS weight_unit
FROM db_bike.01_bronze.bronze_prod a
INNER JOIN db_bike.01_bronze.bronze_prod_text b ON a.PRODUCTID = b.PRODUCTID 
  AND b.LANGUAGE = 'EN'
INNER JOIN db_bike.01_bronze.bronze_prod_cat_text c ON c.PRODCATEGORYID = a.PRODCATEGORYID
WHERE TRIM(a.PRODUCTID) IS NOT NULL OR 
    CAST(TRIM(a.PRICE) AS DECIMAL(10,2)) IS NOT NULL
    OR CAST(TRIM(a.PRICE) AS DECIMAL(10,2)) <> 0