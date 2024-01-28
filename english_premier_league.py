# Importing Packages and Libraries Needed
import numpy as np
import pandas as pd
import datetime
from pymysql import connect
import sqlalchemy
from dotenv import dotenv_values
from prefect import flow, task

def connection():
    config = dotenv_values(".env")
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{config['Access']}@localhost:3306/football_result")
    # engine = sqlalchemy.create_engine('mysql+pymysql://root:OI%4ya#+Tb.Gq(#f@localhost:3306/football_result')
    return engine

headers = ['Date','HomeTeam','AwayTeam','FTHG','FTAG','FTR','HTHG','HTAG','HTR','Referee',
                   'HS','AS','HST','AST','HF','AF','HC','AC','HY','AY','HR','AR']
@task
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

    return all_data
@task
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

    query = 'select max(Date) as Last_Date from football_result.premier_leauge_new'
    last_date = pd.read_sql(query,connection())
    last_date = pd.to_datetime(last_date['Last_Date'].values[0])

    # current_season_last_date = current_season['Date'].max()
    current_season = current_season[(current_season['Date'] >last_date)]

    return current_season

@task
def transform(df):

    name_map = {'FTHG':'FullTimeHomeGoals','FTAG' : 'FullTimeAwayGoals','FTR' : 'FullTimeResults','HTHG' : 'HalfTimeHomeGoals',
                    'HTAG' : 'HalfTimeAwayGoals','HTR': 'HalfTimeResults','HS' : 'HomeShots','AS' : 'AwayShots','HST' : 'HomeShotsTarget',
                    'AST' : 'AwayShotsTarget','HF' : 'HomeFouls','AF' : 'AwayFouls','HC' : 'HomeCorners','AC' : 'AwayCorners',
                    'HY' : 'HomeYellow','AY' : 'AwayYellow','HR' : 'HomeRed','AR' : 'AwayRed'}

    # Renaming the columns into easy to understand formats
    df.rename(columns=name_map, inplace=True)

    points_conditions = [(df['FullTimeResults'] == "H"),(df['FullTimeResults'] == "A"),(df['FullTimeResults'] == "D")]

    home_assigned_points = [3,0,1]
    away_assigned_points = [0,3,1]

    win_team_condition = [df['HomeTeam'],df['AwayTeam'],0]
    lose_team_condition = [df['AwayTeam'],df['HomeTeam'],0]

    result_state = [(df['FullTimeResults'] == df['HalfTimeResults']), (df['HalfTimeResults'] != df['FullTimeResults'])]

    result_state_detail = [
        (df['FullTimeResults'] == df['HalfTimeResults']),
        (df['HalfTimeResults'] == "D") & (df['FullTimeResults'] == "H"),
        (df['HalfTimeResults'] == "D") & (df['FullTimeResults'] == "A"),
        (df['HalfTimeResults'] == "H") & (df['FullTimeResults'] == "A"),
        (df['HalfTimeResults'] == "A") & (df['FullTimeResults'] == "H"),
        (df['HalfTimeResults'] == "H") & (df['FullTimeResults'] == "D"),
        (df['HalfTimeResults'] == "A") & (df['FullTimeResults'] == "D")]

    result_state_evol = ["SMR","CMR"]
    result_state_detail_evol = [df['HalfTimeResults'],"DH","DA","HA","AH","HD","AD"]

    df['HomePoints'] = np.select(points_conditions,home_assigned_points)
    df['AwayPoints'] = np.select(points_conditions,away_assigned_points)
    df['WinTeam'] = np.select(points_conditions,win_team_condition)
    df['LoseTeam'] = np.select(points_conditions,lose_team_condition)
    df['HomeGoals'] = df['FullTimeHomeGoals'] - df['FullTimeAwayGoals']
    df['AwayGoals'] = df['FullTimeAwayGoals'] - df['FullTimeHomeGoals']
    df['ResultEvolution'] = np.select(result_state,result_state_evol)
    df['ResultEvolutionDetails'] = np.select(result_state_detail,result_state_detail_evol)

    df['HomeCards'] = df['HomeYellow'] + df['HomeRed']
    df['AwayCards'] = df['AwayYellow'] + df['AwayRed']
    df['TotalYellow'] = df['HomeYellow'] + df['AwayYellow']
    df['TotalRed'] = df['HomeRed'] + df['AwayRed']
    
    df['%_HomeShotTarget'] = np.where(df['HomeShotsTarget'] < df['HomeShots'], 
                                        (df['HomeShotsTarget'].div(df['HomeShots'])*100).apply(lambda x: round(x, 0)), (df['HomeShots'].div(df['HomeShotsTarget'])*100).apply(lambda x: round(x, 2)))
    df['%_AwayShotTarget'] = np.where(df['AwayShotsTarget'] < df['AwayShots'], 
                                            (df['AwayShotsTarget'].div(df['AwayShots'])*100).apply(lambda x: round(x, 0)), (df['AwayShots'].div(df['AwayShotsTarget'])*100).apply(lambda x: round(x, 2)))
    df = df.replace([np.inf, -np.inf], 0)
    
    return df

@flow
def data_pull(type):
    df = pd.DataFrame()
    if type == 1:
        print("Pulling All Past EPL Data")
        df = past_data()
        print("Transforming Data")
        df = transform(df)
        print("Saving Data")   
        df.to_sql(name= 'premier_leauge_new', con=connection(),index=False,if_exists='replace')
    elif type ==2:
        print("Pulling Lastest Past EPL Data")
        df = weekly_stats()
        print("Transforming Data")
        df = transform(df)   
        print("Saving Data")     
        df.to_sql(name= 'premier_leauge_new', con=connection(),index=False,if_exists='append')

if __name__ == "__main__":
    print('Pulling Latest Data')
    data_pull(1)
    print('Complete')