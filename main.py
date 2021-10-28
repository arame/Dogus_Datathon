from config import Hyper
from helper import Helper
from database import Data

def main():
    Helper.printline("** Started")
    Helper.check_directory(Hyper.sql_dir)
    Helper.check_directory(Hyper.output_dir)
    d = Data()
    if Hyper.create_schema:
        d.create_database()
   
    Helper.printline("** Ended")

if __name__ == "__main__":
    main()

