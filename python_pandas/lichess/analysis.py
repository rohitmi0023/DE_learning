# %%
import pandas as pd
import pandasql as ps
import sqlalchemy as sa

df = pd.read_csv('games.csv')
dropped_columns = ['moves','opening_eco']
df.drop(columns=dropped_columns, inplace=True)
# # alternative
# df_dropped = df.drop(dropped_columns,axis=1)

# DataFrames & Series Basics

# task 1: show df dimensions and column data type
# print('Rows and columns- ',df.shape, '\n')
# print('Columns data types- ', df.dtypes, '\n')

# task 2: create a series of "winner" values. Calculate counts of each type (white/black/draw)
winner_series = df['winner']
win_counts = winner_series.value_counts()
# print('Win counts: ', win_counts, '\n')

# Data Cleaning & Transformation

# task 3: check for missing values in the entire df columns
na_values = df.isnull().sum() # sums up NA count
df_clean = df.dropna(how='any') # pass subset if checking for specific columns

# task 4: convert 'created_at', 'last_move_at' columns to datetime
df['created_at'] = pd.to_datetime(df['created_at'])
df['last_move_at'] = pd.to_datetime(df['last_move_at'])

# task 5: create a new column "rating_difference"
df['rating_difference'] = abs(df['white_rating']-df['black_rating'])

# DateTime Operations
# task 6: extract year/month from "created_at"
df['year'] = df['created_at'].dt.year
df['month'] = df['created_at'].dt.month_name()
# task 7: find day with most games
games_per_day = df['created_at'].dt.day_name().value_counts()

# Merging/Joining Datasets

# task 8: split data into two dfs: white vs black
white_df = df[['id','white_id','white_rating','victory_status']].copy()
black_df = df[['id','black_id','black_rating','victory_status']].copy()
# merge them back using id
merged_df = pd.merge(white_df, black_df, on='id',suffixes=("_white","_black"), how='inner')

# SQL ↔ Pandas Integration

# task 9
# sql query for average rating by player color
query = '''
select
winner
,avg(case when winner = 'white' then white_rating else black_rating end) as avg_winning_rating
from df
where winner != 'draw'
group by winner
'''
avg_color_rating = ps.sqldf(query)
# task 10: top 10 openings by win rate for white
query2 = '''
select
opening_name, 
count(*) as total_games,
sum(case when winner = 'white' then 1 else 0 end)*1.0/count(*) as white_win_rate
from df
group by opening_name
having total_games > 100
order by 3 desc
limit 10
'''
highest_win_rate = ps.sqldf(query2)

# Others
# task 11: time control analysis
df[['base','incremental']] = df['increment_code'].str.split('+',expand=True).astype(int)

# task 12: do higher rated players win faster?
op1 = df.groupby('winner').agg({'rating_difference':'mean', 'turns':'mean'}).rename(columns={'rating_difference':'avg_rating_diff','turns':'avg_turns'
})
# alternative method
op2 = df.groupby('winner').agg(avg_rating_diff=('rating_difference','mean'), avg_turns=('turns','mean'))

# task 13: do white players have significant advantage
print((df.groupby('winner')['id'].count()/df['id'].count())*100)
# -> white has a winning advantage of 4.4% 

# task 14: how do time control affect game outcomes having more than 50 games?
draws_by_increment = df[df['winner']=='draw'].groupby('incremental').size().reset_index(name='draw_count').sort_values(by='draw_count',ascending=False)
total_games_by_increment = df.groupby('incremental').size().reset_index(name='total_games').sort_values(by='total_games',ascending=False)
merged_draw = pd.merge(draws_by_increment,total_games_by_increment,on='incremental',how='inner')
merged_draw = merged_draw[pd.merge(draws_by_increment,total_games_by_increment,on='incremental',how='inner')['total_games'] > 50]
merged_draw['draw_percentage'] = (merged_draw['draw_count']/merged_draw['total_games']*1.0)*100.0
merged_draw.sort_values(by='draw_percentage', ascending=False, inplace=True)

