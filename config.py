
class Hyper:
    db = "./sql/dogus_datathon.db"
    language = "en"
    create_schema = True
    IsStartAgain = True
    IsLoadCustomers = True
    IsLoadSales = True
    customer_file = "./files/FINAL_CUSTOMER_DATATHON.csv"
    customer_columns = ["base_customer_id", "customer_id", "gender", "gender_id", "marital_status", "marital_status_id", "birth_year", "city", "occupation"]
    customer_reorder_columns = ["customer_id", "base_customer_id", "gender", "gender_id", "marital_status", "marital_status_id", "birth_year", "city", "occupation"]
    customer_columns_numeric = ["customer_id", "base_customer_id", "gender_id", "marital_status_id", "birth_year"]
    sales_file = "./files/FINAL_SALES_FILE_DATATHON.csv"
    sales_columns = ["customer_id", "sales_file_id", "sales_file_create_date", "status", "req_brand_code", "req_top_model_code"]
    customer_history_file = "./files/FINAL_CUSTOMER_RELATED_TABLE_FOR_DATATHON.csv"
    customer_history_columns = ["base_customer_id", "vehicle_id", "start_date", "end_date", "status_id", "status_explanation"]
    customer_sales_file = "./files/FINAL_SIFIR_ARAC_ALANLAR_DATATHON.csv"
    customer_sales_columns = ["vehicle_id", "customer_id", "create_date"]
    customer_sales_reorder_columns = ["customer_id", "vehicle_id", "create_date"]
    vehicle_file = "./files/FINAL_VEHICLE_TABLE_DATATHON.csv"
    vehicle_columns = ["vehicle_id", "traffic_date", "brand_code", "topmodel_code", "basemodel_code", "motor_gas_type", "gear_box_type"]
    vehicle_maintenance_file = "./files/MASK_SERVIS_BAKIM_DATATHON_FINAL.csv"
    vehicle_maintenance_columns = ["vehicle_id", "create_date", "total_amount", "is_maintenance"]
    output_dir = "./output_file"
    sql_dir = "./sql"
       
 