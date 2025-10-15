/*  
    Description
        - Creating a stream orders header table from bronze table
*/

CREATE OR REFRESH STREAMING TABLE order_headers
(
    CONSTRAINT valid_sales_id EXPECT(sales_order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
    ,CONSTRAINT valid_date EXPECT(transaction_date IS NOT NULL)
)
SCHEDULE EVERY 1 hour
COMMENT "Cleaned and validated order headers records"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    TRIM(SALESORDERID) AS sales_order_id
    ,CAST(TRIM(CREATEDAT) AS INT) AS transaction_date
    ,TRIM(PARTNERID) AS partner_id
    ,CAST(TRIM(GROSSAMOUNT) AS DECIMAL(10, 2)) AS total_gross_amount
    ,CAST(TRIM(NETAMOUNT) AS DECIMAL(10, 2)) AS total_net_amount
    ,CAST(TRIM(TAXAMOUNT) AS DECIMAL(10, 2)) AS total_tax_amount
    ,CASE 
        TRIM(LIFECYCLESTATUS)
        WHEN 'C' THEN 'Completed Order'
        WHEN 'X' THEN 'Cancelled Order'
        WHEN 'I' THEN 'Incomplete Order'
        ELSE NULL
    END AS lifecycle_status
    ,CASE 
        TRIM(BILLINGSTATUS)
        WHEN 'C' THEN 'Billed'
        WHEN 'X' THEN 'Cancelled Bill'
        WHEN 'I' THEN 'Unbilled'
        ELSE NULL
    END AS billing_status
    ,CASE
        TRIM(DELIVERYSTATUS)
        WHEN 'C' THEN 'Order Delivered'
        WHEN 'X' THEN 'Cancelled Develvery'
        WHEN 'I' THEN 'Incomplete Delivery'
        ELSE NULL
    END AS delivery_status
    ,TRIM(CREATEDBY) AS created_by
    ,CASE 
        WHEN TRIM(BILLINGSTATUS) <> 'X' THEN 1
        ELSE 0
    END AS is_cancelled
FROM 
    STREAM(db_bike.01_bronze.bronze_sales_order)
