import sqlite3 as lite
import sys
import pandas as pd
from helper import Helper
from config import Hyper

class Data:
    TABLE_PARAMETER = "{TABLE_PARAMETER}"
    DROP_TABLE_SQL = f"DROP TABLE {TABLE_PARAMETER};"
    GET_TABLES_SQL = "SELECT name FROM sqlite_master WHERE type='table';"
    VIEW_PARAMETER = "{VIEW_PARAMETER}"
    DROP_VIEW_SQL = f"DROP VIEW {VIEW_PARAMETER};"
    GET_VIEWS_SQL = "SELECT name FROM sqlite_master WHERE type='view';"
    display_customer_table = "select * from customers;"
        
    def __init__(self) -> None:
        self.db = Hyper.db
        self.create_connection()
        Helper.printline("Database opened successfully")  

    def create_connection(self):
        """ create a database connection to the SQLite database
            :param:
            :return:
        """
        '''
        sqlite3.PARSE_DECLTYPES: The sqlite3 module parses the declared type for each column it returns 
            then uses the type converters dictionary to execute the converter function registered for that type there.

        sqlite3.PARSE_COLNAMES: The SQLite interface parses the column name for each column it returns. 
            It will use the converters dictionary and then use the converter function found there to return the value.
        '''
        self.con = None
        try:
            self.con = lite.connect(self.db, detect_types=lite.PARSE_DECLTYPES | lite.PARSE_COLNAMES)
        except Exception as e:
            sys.exit(f"Error with database connection: {e}")
            
    def create_database(self):
        self.df_customers = self.read_file(Hyper.customer_file, Hyper.customer_columns, "customers")
        self.df_customers = self.df_customers[Hyper.customer_reorder_columns]
        self.df_customers[Hyper.customer_columns_numeric] = self.df_customers[Hyper.customer_columns_numeric].apply(pd.to_numeric)  
        self.df_sales_file = self.read_file(Hyper.sales_file, Hyper.sales_columns, "sales")
        self.df_customer_history = self.read_file(Hyper.customer_history_file, Hyper.customer_history_columns, "customer history")  
        self.df_customer_sales = self.read_file(Hyper.customer_sales_file, Hyper.customer_sales_columns, "customer sales")
        self.df_customer_sales = self.df_customer_sales[Hyper.customer_sales_reorder_columns] 
        self.df_vehicles = self.read_file(Hyper.vehicle_file, Hyper.vehicle_columns, "vehicles")  
        self.df_vehicle_maintenance = self.read_file(Hyper.vehicle_maintenance_file, Hyper.vehicle_maintenance_columns, "vehicle maintenace")     
        if Hyper.IsStartAgain:
            self.delete_all_tables()
            self.create_all_tables()
             

        

    def read_file(self, file, columns, file_name):
        try:
            df = pd.read_csv(file, encoding='latin-1', index_col=False, dtype='unicode')
            df = df.drop("Unnamed: 0", axis=1)  # Remove unnamed columns
            df.columns = columns
            return df
        except Exception as e:
            sys.exit(f"Error with reading the file {file_name}: {e}")
           
          
    def delete_all_tables(self):
        tables = self.get_tables()
        self.delete_tables(tables)
        Helper.printline("Deleted all tables")
        
    def delete_all_views(self):
        views = self.get_views()
        self.delete_views(views)
        Helper.printline("Deleted all views")
    
    def get_tables(self):
        c = self.con.cursor()
        c.execute(Data.GET_TABLES_SQL)
        tables = c.fetchall()
        c.close()
        return tables

    def delete_tables(self, tables):
        c = self.con.cursor()
        for table, in tables:
            sql = Data.DROP_TABLE_SQL.replace(Data.TABLE_PARAMETER, table)
            c.execute(sql)
        c.close()
        
    def get_views(self):
        c = self.con.cursor()
        c.execute(Data.GET_VIEWS_SQL)
        views = c.fetchall()
        c.close()
        return views

    def delete_views(self, views):
        c = self.con.cursor()
        for view, in views:
            sql = Data.DROP_VIEW_SQL.replace(Data.VIEW_PARAMETER, view)
            c.execute(sql)
        c.close()
    
    def create_all_tables(self):
        self.create_customers_table()
        self.create_sales_file_table()
        self.create_customer_history_table()
        self.create_customer_sales_table() 
        self.create_vehicle_table()
        self.create_vehicle_maintenance_orig_table() 
        #self.create_vehicle_maintenance_table()    

    def create_customers_table(self):
        self.table_name = "customers"        
        sql_script = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                customer_id integer NOT NULL PRIMARY KEY,
                                base_customer_id integer NOT NULL,
                                gender text NULL,
                                gender_id integer NULL,
                                marital_status text NULL,
                                marital_status_id integer NULL,
                                birth_year integer NULL,
                                city text NULL,
                                occupation text NULL
                            ); """

        self.create_table(sql_script)
        sql_script = f"""INSERT INTO {self.table_name}
                          (customer_id, base_customer_id, gender, gender_id, marital_status, marital_status_id, birth_year, city, occupation) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        self.load_table(self.df_customers, sql_script)
        self.display_table()
    
    def create_sales_file_table(self):
        self.table_name = "sales_file"        
        sql_script = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                customer_id integer NOT NULL,
                                sales_file_id integer NOT NULL,
                                sales_file_create_date timestamp NOT NULL,
                                status integer NOT NULL,
                                req_brand_code text NOT NULL,
                                req_top_model_code integer NULL,
                                FOREIGN KEY(customer_id) REFERENCES customer(customer_id)
                            ); """

        self.create_table(sql_script)
        sql_script = f"""INSERT INTO {self.table_name}
                          (customer_id, sales_file_id, sales_file_create_date, status, req_brand_code, req_top_model_code) 
                          VALUES (?, ?, ?, ?, ?, ?);"""
        self.load_table(self.df_sales_file, sql_script)
        self.display_table()
        
    def create_customer_history_table(self):
        self.table_name = "customer_history"        
        sql_script = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                base_customer_id integer NOT NULL,
                                vehicle_id integer NOT NULL,
                                start_date timestamp NOT NULL,
                                end_date timestamp NULL,
                                status_id integer NOT NULL,
                                status_explanation text NOT NULL
                            ); """

        self.create_table(sql_script)
        sql_script = f"""INSERT INTO {self.table_name}
                          (base_customer_id, vehicle_id, start_date, end_date, status_id, status_explanation) 
                          VALUES (?, ?, ?, ?, ?, ?);"""
        self.load_table(self.df_customer_history, sql_script)
        #self.display_table()
    
    def create_customer_sales_table(self):
        self.table_name = "customer_sales"
        sql_script = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                customer_id integer NOT NULL,
                                vehicle_id integer NOT NULL,
                                create_date timestamp NOT NULL,
                                FOREIGN KEY(vehicle_id) REFERENCES vehicle(vehicle_id),
                                FOREIGN KEY(customer_id) REFERENCES customer(customer_id)
                            ); """
        
        self.create_table(sql_script)
        sql_script = f"""INSERT INTO {self.table_name}
                          (customer_id, vehicle_id, create_date) 
                          VALUES (?, ?, ?);"""
        self.load_table(self.df_customer_sales, sql_script)
        self.display_table()
        
    def create_vehicle_table(self):
        self.table_name = "vehicle"
        sql_script = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                vehicle_id integer NOT NULL PRIMARY KEY,
                                traffic_date timestamp NULL,
                                brand_code text NOT NULL,
                                basemodel_code integer NOT NULL,
                                topmodel_code integer NOT NULL,
                                motor_gas_type text NULL,
                                gear_box_type text NULL
                            ); """
        
        self.create_table(sql_script)
        sql_script = f"""INSERT INTO {self.table_name}
                          (vehicle_id, traffic_date, brand_code, basemodel_code, topmodel_code, motor_gas_type, gear_box_type) 
                          VALUES (?, ?, ?, ?, ?, ?, ?);"""
        self.load_table(self.df_vehicles, sql_script)
        #self.display_table()
        
    def create_vehicle_maintenance_orig_table(self):
        self.table_name = "vehicle_maintenance_orig"
        sql_script = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                create_date timestamp NOT NULL,
                                is_maintenance integer NOT NULL,
                                vehicle_id integer NOT NULL,
                                total_amount integer NULL,
                                FOREIGN KEY(vehicle_id) REFERENCES vehicle(vehicle_id)
                            ); """
        
        self.create_table(sql_script)
        sql_script = f"""INSERT INTO {self.table_name}
                          (create_date, is_maintenance, vehicle_id, total_amount) 
                          VALUES (?, ?, ?, ?);"""
        self.load_table(self.df_vehicle_maintenance, sql_script)
        self.display_table()
        
    def create_vehicle_maintenance_table(self):
        self.table_name = "vehicle_maintenance"
        sql_script = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                vehicle_id integer NOT NULL PRIMARY KEY,
                                first_service timestamp NULL,
                                last_service timestamp NULL,
                                total_service integer NOT NULL,
                                full_amount_service integer NOT NULL,
                                first_maintenance timestamp NULL,
                                last_maintenance timestamp NULL,
                                total_maintenance integer NOT NULL,
                                full_amount_maintenance integer NOT NULL
                            ); """
        
        self.create_table(sql_script)
        sql_script = f"""INSERT INTO {self.table_name}
                            SELECT M.vehicle_id, 
                            first_service, last_service, IFNULL(total_service, 0) AS total_service, IFNULL(full_amount_service, 0) as full_amount_service, 
                            first_maintenance, last_maintenance, IFNULL(total_maintenance, 0) as total_service, IFNULL(full_amount_maintenance, 0) as full_amount_maintenance 
                            FROM
                            (SELECT vehicle_id,
                            min(create_date) as first_maintenance,
                            max(create_date) as last_maintenance,
                            count(*) as total_maintenance,
                            sum(total_amount) as full_amount_maintenance
                            FROM vehicle_maintenance_orig
                            WHERE is_maintenance = 1
                            GROUP BY vehicle_id) AS M
                            LEFT OUTER JOIN 
                            (SELECT vehicle_id,
                            min(create_date) as first_service,
                            max(create_date) as last_service,
                            count(*) as total_service,
                            sum(total_amount) as full_amount_service
                            FROM vehicle_maintenance_orig
                            WHERE is_maintenance = 0
                            GROUP BY vehicle_id) AS S
                            ON M.vehicle_id = S.vehicle_id
                            UNION
                            SELECT S.vehicle_id, 
                            first_service, last_service, IFNULL(total_service, 0) AS total_service, IFNULL(full_amount_service, 0) as full_amount_service, 
                            first_maintenance, last_maintenance, IFNULL(total_maintenance, 0) as total_service, IFNULL(full_amount_maintenance, 0) as full_amount_maintenance 
                            FROM
                            (SELECT vehicle_id,
                            min(create_date) as first_service,
                            max(create_date) as last_service,
                            count(*) as total_service,
                            sum(total_amount) as full_amount_service
                            FROM vehicle_maintenance_orig
                            WHERE is_maintenance = 0
                            GROUP BY vehicle_id) AS S
                            LEFT OUTER JOIN 
                            (SELECT vehicle_id,
                            min(create_date) as first_maintenance,
                            max(create_date) as last_maintenance,
                            count(*) as total_maintenance,
                            sum(total_amount) as full_amount_maintenance
                            FROM vehicle_maintenance_orig
                            WHERE is_maintenance = 1
                            GROUP BY vehicle_id) AS M
                            ON M.vehicle_id = S.vehicle_id        
                          """
        success_message = f"Script for loading {self.table_name} completed"
        error_message = f"Error loading {self.table_name}"
        self.execute_script(sql_script, success_message, error_message)
        
    def create_table(self, create_table_sql):
        """ create a table from the create_table_sql statement
            :param  create_table_sql : a CREATE TABLE statement
            :return:
        """
        try:
            c = self.con.cursor()
            c.execute(create_table_sql)
            c.close()
            Helper.printline(f"{self.table_name} table successfully created")
        except Exception as e:
            sys.exit(f"Error with table creation for {self.table_name}: {e}")
    
    def load_table(self, df, sql_script):
        """ load the countries from the csv file into the database
            :param  df          : dataframe to load the data
                    sql_script  : script to load the dataframe into the table       
            :return:
        """
        # convert the dataframe to a list of tuples so that the data
        # can be loaded into the table
        l=tuple(df.itertuples(index=False, name=None))
 
        try:
            c = self.con.cursor()
            c.executemany(sql_script, l)
            c.close()
            self.con.commit()
            Helper.printline(f"{self.table_name} table loaded")
        except Exception as e:
            sys.exit(f"Error with inserting {self.table_name}: {e}")
     
    def execute_script(self, sql_script, success_message, error_message):
        try:
            c = self.con.cursor()
            c.execute(sql_script)
            c.close()
            self.con.commit()
            Helper.printline(success_message)
        except Exception as e:
            sys.exit(f"{error_message}: {e}")
    
    def create_all_views(self):
        self.create_customer_history_view()
        self.create_sales_file_view()
        self.create_vehicle_view()
        self.create_vehicle_maintenance_orig_view()
        self.create_vehicle_mainenance_view() 
        
    def create_customer_history_view(self):
        self.view_name = "vw_customer_history"
        sql_script = f''' CREATE VIEW IF NOT EXISTS {self.view_name} AS
                            SELECT 
                            base_customer_id,
                            vehicle_id,
                            start_date,
                            CASE WHEN end_date <= date('{Hyper.End_Date_Period}') THEN end_date ELSE NULL END AS end_date,
                            status_id,
                            status_explanation 
                            FROM customer_history
                            WHERE start_date <= date('{Hyper.End_Date_Period}')
                        '''
        self.execute_view_script(sql_script)
        
    def create_sales_file_view(self):
        self.view_name = "vw_sales_file"
        sql_script = f''' CREATE VIEW IF NOT EXISTS {self.view_name} AS
                            SELECT * FROM sales_file
                            WHERE sales_file_create_date <= date('{Hyper.End_Date_Period}')
                        '''
        self.execute_view_script(sql_script)
        
    def create_vehicle_view(self):
        self.view_name = "vw_vehicle"
        sql_script = f''' CREATE VIEW IF NOT EXISTS {self.view_name} AS
                            SELECT * FROM vehicle
                            WHERE traffic_date <= date('{Hyper.End_Date_Period}')
                        '''
        self.execute_view_script(sql_script)

    def create_vehicle_maintenance_orig_view(self):
        self.view_name = "vw_vehicle_maintenance_orig"
        sql_script = f''' CREATE VIEW IF NOT EXISTS {self.view_name} AS
                            SELECT * FROM vehicle_maintenance_orig
                            WHERE create_date <= date('{Hyper.End_Date_Period}')
                        '''
        self.execute_view_script(sql_script)
                                
    def create_vehicle_mainenance_view(self):
        self.view_name = "vw_vehicle_maintenance"
        view_used = "vw_vehicle_maintenance_orig"
        sql_script = f""" CREATE VIEW IF NOT EXISTS {self.view_name} AS
                            SELECT M.vehicle_id, 
                            first_service, last_service, IFNULL(total_service, 0) AS total_service, IFNULL(full_amount_service, 0) as full_amount_service, 
                            first_maintenance, last_maintenance, IFNULL(total_maintenance, 0) as total_service, IFNULL(full_amount_maintenance, 0) as full_amount_maintenance 
                            FROM
                            (SELECT vehicle_id,
                            min(create_date) as first_maintenance,
                            max(create_date) as last_maintenance,
                            count(*) as total_maintenance,
                            sum(total_amount) as full_amount_maintenance
                            FROM {view_used}
                            WHERE is_maintenance = 1
                            GROUP BY vehicle_id) AS M
                            LEFT OUTER JOIN 
                            (SELECT vehicle_id,
                            min(create_date) as first_service,
                            max(create_date) as last_service,
                            count(*) as total_service,
                            sum(total_amount) as full_amount_service
                            FROM {view_used}
                            WHERE is_maintenance = 0
                            GROUP BY vehicle_id) AS S
                            ON M.vehicle_id = S.vehicle_id
                            UNION
                            SELECT S.vehicle_id, 
                            first_service, last_service, IFNULL(total_service, 0) AS total_service, IFNULL(full_amount_service, 0) as full_amount_service, 
                            first_maintenance, last_maintenance, IFNULL(total_maintenance, 0) as total_service, IFNULL(full_amount_maintenance, 0) as full_amount_maintenance 
                            FROM
                            (SELECT vehicle_id,
                            min(create_date) as first_service,
                            max(create_date) as last_service,
                            count(*) as total_service,
                            sum(total_amount) as full_amount_service
                            FROM {view_used}
                            WHERE is_maintenance = 0
                            GROUP BY vehicle_id) AS S
                            LEFT OUTER JOIN 
                            (SELECT vehicle_id,
                            min(create_date) as first_maintenance,
                            max(create_date) as last_maintenance,
                            count(*) as total_maintenance,
                            sum(total_amount) as full_amount_maintenance
                            FROM {view_used}
                            WHERE is_maintenance = 1
                            GROUP BY vehicle_id) AS M
                            ON M.vehicle_id = S.vehicle_id
                        """
        self.execute_view_script(sql_script) 
   
    def execute_view_script(self, sql_script):
        try:
            c = self.con.cursor()
            c.execute(sql_script)
            c.close
            Helper.printline(f"View successfully created: {self.view_name}")
        except Exception as e:
            sys.exit(f"Error creating view {self.view_name}: " + e)
           
    def display_table(self):
        sql_script = f"""SELECT * FROM {self.table_name}"""
        try:
            c = self.con.cursor()
            c.execute(sql_script)
    
            # View result
            result = c.fetchall()
        except Exception as e:
            sys.exit(f"Error with retreiving {self.table_name}: {e}")
            
        self.print_results(result)
        c.close()

    def print_results(self, result):
        limit = len(result)
        if limit > 10:
            Helper.printline(f"Contents of {self.table_name} table, first 10 out of {limit} rows.")
            i = 0
            for row in result:
                i += 1
                if i > 10:
                    break
                
                Helper.printline(f"{i}: {row}")
        else:
            Helper.printline(f"Contents of {self.table_name} table, all {limit} rows.")
            i = 0
            for row in result:
                i += 1
                Helper.printline(f"{i}: {row}")
 
       