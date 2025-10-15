import dlt

dlt.table(
    name="employees"
)
def employees():
    return
        (
             dlt.read("db_bike.01_bronze.bronze_addresses")
        )

