import time, datetime, os
from os import listdir
import calendar

class Helper:
    def printline(text):
        _date_time = time.strftime('%Y/%m/%d %H:%M:%S')
        print(f"{_date_time}   {text}")
        
    def string_to_date(_date):
        _date = _date.replace("_", "-")
        format = "%Y-%m-%d"
        date = datetime.datetime.strptime(_date, format)
        return date
    
    def find_csv_filenames( path_to_dir, suffix=".csv" ):
        filenames = listdir(path_to_dir)
        return [ filename for filename in filenames if filename.endswith( suffix ) ]
    
    def remove_non_ascii_characters(string_unicode):
        string_encode = string_unicode.encode("ascii", "ignore")
        string_decode = string_encode.decode()
        return string_decode
    
    def convert_string_to_date(input):
        # Typical date is "Sun Apr 04 00:05:28 +0000 2021"
        year = int(input[-4:])
        abbr_to_num = {name: num for num, name in enumerate(calendar.month_abbr) if num}
        month_x = input[4:7]
        month = abbr_to_num[month_x]
        day = int(input[8:10])
        date = datetime.datetime(year, month, day)
        return date

    def check_directory(directory):
        if os.path.exists(directory):
            return

        os.mkdir(directory)
        
        