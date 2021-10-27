import datetime as dt
import pandas as pd
from config import Hyper
from helper import Helper
from database import Data

'''
    This program is the first in a suite of programs to be executed in this order
    1/ App - gets tweets from Twitter API
    2/ Location - gets the country of the tweet from user location
    3/ Annotate - calculates the sentiment of each tweet
    4/ Wordcload - shows the words most in use in tweets from different countries
    5/ Datapreparation - gets the data in the correct form
    6/ Transformer - builds a transformer model from the tweets
'''

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

