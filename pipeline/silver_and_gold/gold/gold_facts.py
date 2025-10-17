import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def mask_email(email):
    if email:
        at_index = email.find('@')
        if at_index != -1:
            return email[0] + "*" * (at_index - 2) + email[at_index-1:]
    return None

@dlt.table(
    name='dim_employees'
    ,comment='Employee dimension table'
    ,table_properties={"quality": "gold"}
)
@dlt.expect("valid_employee_id", "employee_sk IS NOT NULL")
@dlt.expect_or_fail("valid_name", "full_name IS NOT NULL")
def dim_employees():
    employees = dlt.read("db_bike.02_silver.employees")
      
    dim_df = (
            employees
                .withColumn("employee_sk", F.xxhash64(F.col("employee_id")).cast("String"))
                .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
                .withColumn("mask_email", mask_email(F.col("email_address")))
                .select(
                    "employee_sk"
                    ,"employee_id"
                    ,"full_name"
                    ,"mask_email"
                    ,"gender"
                    ,"country_name"
                    ,"region_name"
                )
                .filter(F.col("is_active") == 1)
        )
    
    return dim_df

@dlt.table(
    name="dim_partners"
    ,comment="Partner dimension table"
    ,table_properties={"quality": "gold"}
)
@dlt.expect("valid_partner_id", "partner_sk IS NOT NULL")
@dlt.expect("valid_company_name", "company_name IS NOT NULL")
def dim_partners():
    partners = dlt.read("db_bike.02_silver.business_partners")

    dim_df = (
            partners
                .withColumn("partner_sk", F.xxhash64(F.col("partner_id")).cast("String"))
                .withColumn("mask_email", mask_email(F.col("email_address")))
                .select(
                    "partner_sk"
                    ,"partner_id"
                    ,"company_name"
                    ,"mask_email"
                    ,"country_name"
                    ,"region_name"
                    ,"phone_number"
                    ,"web_address")
    )

    return dim_df

@dlt.table(
    name="dim_products"
    ,comment="Product dimension table"
    ,table_properties={"quality": "gold"}
)
def dim_products():
    products = dlt.read("db_bike.02_silver.products")

    dim_df = (
            products
                .withColumn("product_sk", F.xxhash64(F.col("product_id")).cast("String"))
                .select(
                    "product_sk"
                    ,"product_id"
                    ,"partner_id"
                    ,"product_description"
                    ,"product_category"
                    ,"list_price"
                    ,"currency"                
                    )
        )
    
    return dim_df


@dlt.table(
    name="fact_sales"
    ,comment="Sales fact table"
    ,table_properties={"quality": "gold"}
)
@dlt.expect_or_fail("valid_amount", "sales_amount >= 0")
@dlt.expect_or_fail("valid_prod_sk", "product_sk IS NOT NULL")
@dlt.expect("valid_date", "date IS NOT NULL")
@dlt.expect("warn_partner_id", "partner_sk IS NOT NULL")
def fact_sales():
    dim_business_partners = dlt.read("dim_partners").alias("bp")
    dim_products = dlt.read("dim_products").alias("p")
    silver_order_details = dlt.read_stream("db_bike.02_silver.order_details").alias("o")

    fact_df = (
            silver_order_details
                .join(dim_products, F.col("o.product_id") == F.col("p.product_id"), how="inner")
                .join(dim_business_partners, F.expr("bp.partner_id = p.partner_id"), how="left")
                .withColumn("sales_sk", F.xxhash64(F.col("o.sales_order_id"), F.col("o.sales_order_item")).cast("String"))
                .withColumn("sales_amount", F.col("o.net_amount") * F.col("o.quantity"))
                .withColumn("date", F.to_date(F.col("o.delivery_date"), "MM/dd/yyyy"))
                .select(
                    "sales_sk"
                    ,"partner_sk"
                    ,"product_sk"
                    ,"date"
                    ,"sales_amount"  
                )
    )
    return fact_df

@dlt.table(
    name="fact_productivity"
    ,comment="Productivity fact table"
    ,table_properties={"quality": "gold"}
)
def fact_rcm():
        dim_employees = dlt.read("dim_employees").alias("e")
        order_headers = dlt.read_stream("db_bike.02_silver.order_headers").alias("oh")

        fact_df = (
            order_headers
                .join(dim_employees, F.col("oh.created_by") == F.col("e.employee_id"), how="inner")
                .withColumn("date", F.to_date(F.col("oh.transaction_date"), "MM/dd/yyyy"))
                .withColumn("productivity_sk", F.xxhash64(F.col("e.employee_id"), F.col("oh.transaction_date")).cast("String"))
                .groupBy("productivity_sk", "employee_sk", "date")
                .agg(F.count(F.col("oh.sales_order_id")).alias("total_orders"))
                .select(
                    "productivity_sk"
                    ,"employee_sk"
                    ,"date"
                    ,"total_orders"
                )
        )
        return fact_df
