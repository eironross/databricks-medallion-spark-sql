# Used Bike Sales data in Kaggle 
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Addresses schema
address_schema = StructType([
    StructField("address_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("street", StringType(), True),
    StructField("building", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("address_type", IntegerType(), True),
    StructField("validity_startdate", IntegerType(), True),
    StructField("validity_enddate", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
    ])

# Business Partners
business_schema = StructType([
    StructField("partner_id", IntegerType(), True),
    StructField("partner_role", IntegerType(), True),
    StructField("email_address", StringType(), True),
    StructField("phone_number", DoubleType(), True),
    StructField("fax_number", StringType(), True),
    StructField("web_address", StringType(), True),
    StructField("address_id", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("legal_form", StringType(), True),
    StructField("created_by", IntegerType(), True),
    StructField("created_at", IntegerType(), True),
    StructField("changed_by", IntegerType(), True),
    StructField("changed_at", IntegerType(), True),
    StructField("currency", StringType(), True)  
])

# Employees Schema
employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("name_first", StringType(), True),
    StructField("name_middle", StringType(), True),
    StructField("name_last", StringType(), True),
    StructField("name_initials", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("language", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("login_name", StringType(), True),
    StructField("address_id", IntegerType(), True),
    StructField("validity_startdate", IntegerType(), True),
    StructField("validity_enddate", IntegerType(), True)
])

# Products Categories
prod_categories_schema = StructType([
    StructField("prod_category_id", StringType(), True),
    StructField("created_by", IntegerType(), True),
    StructField("created_at", IntegerType(), True)
])

# Products Category Text
prod_cat_text_schema = StructType([
    StructField("prod_category_id", StringType(), True),
    StructField("language", StringType(), True),
    StructField("short_descr", StringType(), True),
    StructField("medium_descr", StringType(), True),
    StructField("long_descr", StringType(), True)
])

# Products Text
prod_text_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("language", StringType(), True),
    StructField("short_descr", StringType(), True),
    StructField("medium_descr", StringType(), True),
    StructField("long_descr", StringType(), True)
])

# Products 
prod_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("type_code", StringType(), True),
    StructField("prod_category_id", StringType(), True),
    StructField("created_by", IntegerType(), True),
    StructField("created_at", IntegerType(), True),
    StructField("change_by", IntegerType(), True),
    StructField("change_at", IntegerType(), True),
    StructField("sup_partner_id", IntegerType(), True),
    StructField("tax_tariff_code", StringType(), True),
    StructField("quantity_unit", StringType(), True),
    StructField("weight_measure", StringType(), True),
    StructField("weight_unit", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("width", StringType(), True),
    StructField("depth", StringType(), True),
    StructField("height", StringType(), True),
    StructField("dimesion_unit", StringType(), True),
    StructField("productpi_curl", StringType(), True),
])

# Sales Items
sales_items_schema = StructType([
    StructField("sales_order_id", IntegerType(), True),
    StructField("sale_order_item", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("note_id", StringType(), True),
    StructField("currency", StringType(), True), 
    StructField("gross_amount", IntegerType(), True),
    StructField("net_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("item_atp_status", StringType(), True),
    StructField("op_items_pos", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("quantity_unit", StringType(), True),
    StructField("delivery_date", IntegerType(), True)
])

# Sales Order
sales_order_schema = StructType([
    StructField("sales_order_id", IntegerType(), True),
    StructField("created_by", IntegerType(), True),
    StructField("created_at", IntegerType(), True),
    StructField("change_by", IntegerType(), True),
    StructField("change_at", IntegerType(), True),
    StructField("fisc_variant", StringType(), True),
    StructField("fiscal_year_period", IntegerType(), True),
    StructField("note_id", StringType(), True),
    StructField("partner_id", IntegerType(), True), 
    StructField("sales_org", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("gross_amount", IntegerType(), True),
    StructField("net_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("life_cycle_status", StringType(), True),
    StructField("billing_status", StringType(), True),
    StructField("delivery_status", StringType(), True),
])
