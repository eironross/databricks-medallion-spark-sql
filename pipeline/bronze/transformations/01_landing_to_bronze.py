import dlt
from utilities.utils import bronze_ingest
from pyspark.sql import functions as F
# from utilities.schemas import address_schema, business_schema, employees_schema, prod_categories_schema, prod_cat_text_schema, prod_text_schema, prod_schema, sales_items_schema, sales_order_schema


# Create a DLT table for Addreses 
@dlt.table(
    name="bronze_addresses"
    ,comment="Ingestion Data of the Addresses table"
    ,table_properties={"quality": "bronze"}
)
def bronze_addresses():
    return bronze_ingest(
        spark
        ,path_name="addresses"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Business Partners 
@dlt.table(
    name="bronze_business_partner"
    ,comment="Ingestion Data of the Business Partners table"
    ,table_properties={"quality": "bronze"}
)
def bronze_business_partner():
    return bronze_ingest(
        spark
        ,path_name="businesspartners"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Employees
@dlt.table(
    name="bronze_employees"
    ,comment="Ingestion Data of the Employees table"
    ,table_properties={"quality": "bronze"}
)
def bronze_employees():
    return bronze_ingest(
        spark
        ,path_name="employees"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Products Categories
@dlt.table(
    name="bronze_prod_cat"
    ,comment="Ingestion Data of the Products Categories table"
    ,table_properties={"quality": "bronze"}
)
def bronze_prod_cat():
    return bronze_ingest(
        spark
        ,path_name="productcategory"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Products Categories Text
@dlt.table(
    name="bronze_prod_cat_text"
    ,comment="Ingestion Data of the Products Categories Text table"
    ,table_properties={"quality": "bronze"}
)
def bronze_prod_cat_text():
    return bronze_ingest(
        spark
        ,path_name="productcategorytext"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Products Text
@dlt.table(
    name="bronze_prod_text"
    ,comment="Ingestion Data of the Products Text table"
    ,table_properties={"quality": "bronze"}
)
def bronze_prod_text():
    return bronze_ingest(
        spark
        ,path_name="producttexts"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Products
@dlt.table(
    name="bronze_prod"
    ,comment="Ingestion Data of the Products table"
    ,table_properties={"quality": "bronze"}
)
def bronze_prod():
    return bronze_ingest(
        spark
        ,path_name="products"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Sales items
@dlt.table(
    name="bronze_sales_items"
    ,comment="Ingestion Data of the Sales items table"
    ,table_properties={"quality": "bronze"}
)
def bronze_sales_items():
    return bronze_ingest(
        spark
        ,path_name="saleorderitems"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))

# Create a DLT table for Sales Order items
@dlt.table(
    name="bronze_sales_order"
    ,comment="Ingestion Data of the Sales Order items table"
    ,table_properties={"quality": "bronze"}
)
def bronze_sales_order():
    return bronze_ingest(
        spark
        ,path_name="salesorders"
        ,format="csv"
    ).withColumn("dwh_source", F.lit("bronze"))