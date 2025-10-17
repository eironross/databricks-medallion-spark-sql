/*  
    Description
        - Creating a stream orders details table from bronze table
*/

CREATE OR REFRESH MATERIALIZED VIEW order_details
(
    CONSTRAINT valid_sales_id EXPECT(sales_order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
    ,CONSTRAINT valid_sales_order_item EXPECT(sales_order_item IS NOT NULL) ON VIOLATION FAIL UPDATE
    ,CONSTRAINT valid_date EXPECT(delivery_date IS NOT NULL)
)
SCHEDULE EVERY 1 hour
COMMENT "Cleaned and validated order details records"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  TRIM(SALESORDERID) AS sales_order_id
  ,TRIM(SALESORDERITEM) AS sales_order_item
  ,CAST(TRIM(DELIVERYDATE) AS INT) AS delivery_date
  ,TRIM(PRODUCTID) AS product_id
  ,CAST(TRIM(GROSSAMOUNT) AS DECIMAL(10,2)) AS gross_amount
  ,CAST(TRIM(NETAMOUNT) AS DECIMAL(10,2)) AS net_amount
  ,CAST(TRIM(TAXAMOUNT) AS DECIMAL(10,2)) AS tax_amount
  ,CAST(TRIM(QUANTITY) AS INT) AS quantity
FROM db_bike.01_bronze.bronze_sales_items
