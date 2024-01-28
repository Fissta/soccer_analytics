# Importing Packages and Libraries Needed
import numpy as np
import pandas as pd
import datetime
from pymysql import connect
import sqlalchemy
# from dotenv import dotenv_values, load_dotenv
import os
from prefect import flow, task
from prefect.blocks.system import Secret


headers = ['Date','HomeTeam','AwayTeam','FTHG','FTAG','FTR','HTHG','HTAG','HTR','Referee',
                   'HS','AS','HST','AST','HF','AF','HC','AC','HY','AY','HR','AR']
name_map = {'FTHG':'FullTimeHomeGoals','FTAG' : 'FullTimeAwayGoals','FTR' : 'FullTimeResults','HTHG' : 'HalfTimeHomeGoals',
                    'HTAG' : 'HalfTimeAwayGoals','HTR': 'HalfTimeResults','HS' : 'HomeShots','AS' : 'AwayShots','HST' : 'HomeShotsTarget',
                    'AST' : 'AwayShotsTarget','HF' : 'HomeFouls','AF' : 'AwayFouls','HC' : 'HomeCorners','AC' : 'AwayCorners',
                    'HY' : 'HomeYellow','AY' : 'AwayYellow','HR' : 'HomeRed','AR' : 'AwayRed'}
    # Database Connection
@task
def connection():
    secret_block = Secret.load("access")

    # Access the stored secret
    config = secret_block.get()
    # config = variables.get('Access')
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{config}@localhost:3306/football_result")
    # engine = sqlalchemy.create_engine('mysql+pymysql://root:OI%4ya#+Tb.Gq(#f@localhost:3306/football_result')
    return engine

# Pull all past data on the English Premier Leauge
def past_data():
    hold = []
    # In order to pull the data we need to get the last two digits of the Premier Leauge year. We sorts to acieve this with two parameters, 
    # after which they were piped into the links to extract the data. All past results were store in all_data
    for year in range(2000, 2024):
        first_part = year % 100
        second_part = first_part + 1
        if second_part < 10:
            # print(f'{first_part}/{second_part}')
            data = pd.read_csv(f'https://www.football-data.co.uk/mmz4281/0{first_part}0{second_part}/E0.csv',
                                usecols=headers, encoding='ISO-8859-1')
            data.insert(0,'Season',f'0{str(first_part)}/0{str(second_part)}')
            hold.append(data)
        else:
            period = str(first_part)+str(second_part)
            # print(period)
            data = pd.read_csv(f'https://www.football-data.co.uk/mmz4281/{period}/E0.csv',
                                usecols=headers, encoding='ISO-8859-1')
            if period == '910':
                data.insert(0,'Season',f'0{first_part}/{second_part}')
            else:
                data.insert(0,'Season',f'{first_part}/{second_part}')
            hold.append(data)

    all_data =  pd.DataFrame
    all_data = pd.concat(hold) 

    # Formating Date
    all_data['Date'] = pd.to_datetime(all_data['Date'], format='mixed', dayfirst=True)

    # Dropping Blank Rows
    all_data.dropna(inplace=True)

    # Renaming the columns into easy to understand formats
    all_data.rename(columns=name_map, inplace=True)

    # Saving to mysql database
    all_data.to_sql(name= 'premier_leauge', con=connection(),index=False,if_exists='replace') 


@flow
# Pulling Match Results
def weekly_stats():
    # Pulling Subsequent results
    # Identify the year and the current month as the season flows into the next year
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month

    # Every football season starts in August, so we seek to identify the period we are in to select the season to pipe into our URL
    if current_month > 7:
        first_period = current_year%100
        second_period = first_period + 1
        period = str(first_period)+str(second_period)
        current_season = pd.read_csv(f'https://www.football-data.co.uk/mmz4281/{period}/E0.csv',
                                        usecols=headers, encoding='ISO-8859-1')
        current_season.insert(0,'Season',f'{first_period}/{first_period}')
    else:
        second_period = current_year%100
        first_period = second_period - 1
        period = str(first_period)+str(second_period)
        current_season = pd.read_csv(f'https://www.football-data.co.uk/mmz4281/{period}/E0.csv',
                                        usecols=headers, encoding='ISO-8859-1')
        current_season.insert(0,'Season',f'{first_period}/{second_period}')  

    current_season['Date'] = pd.to_datetime(current_season['Date'], format='%d/%m/%Y', dayfirst=True)

    query = 'select max(Date) as Last_Date from football_result.premier_leauge'
    last_date = pd.read_sql(query,connection())
    last_date = pd.to_datetime(last_date['Last_Date'].values[0])

    # current_season_last_date = current_season['Date'].max()
    current_season = current_season[(current_season['Date'] >last_date)]

    # Renaming the columns into easy to understand formats
    current_season.rename(columns=name_map, inplace=True)

    # Saving to database
    current_season.to_sql(name= 'premier_leauge', con=connection(),index=False,if_exists='append')



if __name__ == "__main__":
    print('Latest Data')
    weekly_stats()
    print('Complete')