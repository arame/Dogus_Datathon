from config import Hyper
from helper import Helper
from database import Data

def main():
    Helper.printline("** Started")
    Helper.check_directory(Hyper.sql_dir)
    Helper.check_directory(Hyper.output_dir)
    start_date = Hyper.Start_Date_Period.strftime('%B, %Y')
    end_date = Hyper.End_Date_Period.strftime('%B, %Y')
    Helper.printline(f"Dates for predicting new sales file per customer: from {start_date} to {end_date}")
    d = Data()
    if Hyper.create_schema:
        d.create_database()
   
    d.delete_all_views()
    d.create_all_views()
    
    Helper.printline("** Ended")

if __name__ == "__main__":
    main()

