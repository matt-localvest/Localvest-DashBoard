# Web App for Charts - Localvest 
# This is going to be my first little web app thing using streamlit. It's working well so far so just need to stay strong and not give up

from altair.vegalite.v4.schema.channels import StrokeWidth
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import pydeck as pdk
import psycopg2
from sqlalchemy import create_engine
import os
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# # ====== Connection ======
# Connecting to PostgreSQL by providing a sqlachemy engine
password2 = 'Invest.Inf0.18'
conn = psycopg2.connect(database = "localvest", user = "postgres", password = password2, host = "127.0.0.1", port = "5432")
print ("Opened database successfully")

cnx = create_engine('postgresql://postgres:'+password2+'@localhost:5432/localvest')

# Querying from my SQl database to show all Investors that have profiles that haven't been deleted
df_investors = pd.read_sql("""SELECT DISTINCT user_id, email, full_name, account_type, public.user_accounts.deleted_at, 
                              public.user_accounts.created_at, investment_interests, investment_answer_2 
                              FROM public.users JOIN public.user_accounts ON public.user_accounts.user_id = public.users.id 
                              WHERE account_type LIKE 'Investor' AND public.user_accounts.deleted_at IS NULL 
                              AND public.users.encrypted_password NOT LIKE '' 
                              ORDER BY user_id ASC;""", con=cnx)

# Querying from my SQl database to show all Issuers with profiles filled out 
df_issuers = pd.read_sql("""SELECT DISTINCT user_id, email, full_name, account_type, public.user_accounts.deleted_at, 
                            public.user_accounts.created_at, investment_interests, investment_answer_2 
                            FROM public.users JOIN public.user_accounts ON public.user_accounts.user_id = public.users.id 
                            WHERE account_type LIKE 'IssuerAdmin' AND public.user_accounts.deleted_at IS NULL 
                            AND public.users.encrypted_password NOT LIKE '' 
                            ORDER BY user_id ASC;""", con=cnx)

# Deleting the values that repeat for investor accounts getting rid of the same name and user_id
df_investors.drop_duplicates(subset=["user_id"], keep="last", inplace=True)
df_investors.drop_duplicates(subset=["full_name"], keep="last", inplace=True)

# Deleting the values that repeat for issuer accounts getting rid of the same name and user_id
df_issuers.drop_duplicates(subset=["user_id"], keep="last", inplace=True)
df_issuers.drop_duplicates(subset=["full_name"], keep="last", inplace=True)

# Searching for values within the full names and emails to be able to remove the extra esther accounts and any test accounts 
search_values = ['test','Test','DELETE', 'Delete','estherkiim93', 'admin', 'johnbracken', 'Bill Militello', 'Matt Nicklas', 'Laura Calazans', 'Binh Phan']

# Investors
df_removal_title_invest = df_investors[df_investors.full_name.str.contains('|'.join(search_values))]
df_removal_email_invest = df_investors[(df_investors.email.str.contains('|'.join(search_values)))]

# Issuers
df_removal_title_issuer = df_issuers[df_issuers.full_name.str.contains('|'.join(search_values))]
df_removal_email_issuer = df_issuers[(df_issuers.email.str.contains('|'.join(search_values)))]

# Check and see if the search value list is within any of the emails or full_names of the users 

# Investor
cond_invest = df_investors['full_name'].isin(df_removal_title_invest['full_name'])
cond_email = df_investors['email'].isin(df_removal_email_invest['email'])

# Issuer
cond_issuer = df_issuers['full_name'].isin(df_removal_title_issuer['full_name'])
cond_email_2 = df_issuers['email'].isin(df_removal_email_issuer['email'])

# Drop the values from each of the dfs for Investors and Issuers
df_investors.drop(df_investors[cond_invest].index, inplace = True)
df_investors.drop(df_investors[cond_email].index, inplace = True)
df_issuers.drop(df_issuers[cond_issuer].index, inplace = True)
df_issuers.drop(df_issuers[cond_email_2].index, inplace = True)

# Getting total for investors and issuers how many account do we have for equation purposes to display in streamlit
df_investors_total = df_investors['user_id'].count()
df_issuers_total = df_issuers['user_id'].count()

# Drop the na values or will make this the section where I have a loop of some kind to fillna with blank values for investment interests.  
df_investors.dropna(subset = ["investment_answer_2"], inplace=True)
df_investors.dropna(subset = ["investment_interests"], inplace=True)

# Dropping the values in my issuer df so that I can make the dfs for tags and clesses much easier 
df_issuers.dropna(subset = ["investment_answer_2"], inplace=True)
df_issuers.dropna(subset = ["investment_interests"], inplace=True)

# This section will drop columns if we don't want them anymore for certain things. 
df_investors = df_investors.drop(columns = ['deleted_at'])
df_issuers = df_issuers.drop(columns = ['deleted_at'])

# Complete Profiles - Setting the dataframe to have all profiles that have completely finished their profiles. 
df_investor_interests = df_investors[(df_investors["investment_interests"].str.len() != 0) & (df_investors["investment_answer_2"].str.len() != 0)]
df_issuers_interests = df_issuers[(df_issuers["investment_interests"].str.len() != 0) & (df_issuers["investment_answer_2"].str.len() != 0)]

# Total number of profiles that are complete 
total_complete_profiles_investor = df_investor_interests['user_id'].count()
total_complete_profiles_issuer = df_issuers_interests['user_id'].count()

# GETTING THE PERCENT OF THOSE WHO FINISHED THEIR PROFILES
investor_percent = str(round((total_complete_profiles_investor/df_investors_total) * 100)) + '%'
issuer_percent = str(round((total_complete_profiles_issuer/df_issuers_total) * 100)) + '%'

# Dataframe for Investment Types!!!!!!!!
df_investors_types = df_investors[df_investors["investment_answer_2"].str.len() != 0]
df_issuers_types = df_issuers[df_issuers["investment_answer_2"].str.len() != 0]

# Get the count for the total number of investors/issuers that filled out investment type
total_types = df_investors_types['user_id'].count()
total_types_issuers = df_issuers_types['user_id'].count()

# Dataframes for Investment Interests!!!!!!!!! 
df_investors_tags = df_investors[df_investors["investment_interests"].str.len() != 0]
df_issuers_tags = df_issuers[df_issuers["investment_interests"].str.len() != 0]

# Get the count for the total number of investors/Issuers that filled out investment tags
total_tags = df_investors_tags['user_id'].count()
total_tags_issuers = df_issuers_tags['user_id'].count()

# Dataframe for each of the bubbles - Investors
df_investor_bubbles = df_investors_tags.explode("investment_interests")
df_investor_bubbles = df_investor_bubbles.drop(columns = ['investment_answer_2'])

# Dataframe for each of the bubbles - Issuers
df_issuers_bubbles = df_issuers_tags.explode("investment_interests")
df_issuers_bubbles = df_issuers_bubbles.drop(columns = ['investment_answer_2'])

# Investment categories dataframe - Investors
df_investor_types = df_investors_types.explode("investment_answer_2")
df_investor_types = df_investor_types.drop(columns = ['investment_interests'])

# Investment categories dataframe - Issuers
df_issuers_types = df_issuers_types.explode("investment_answer_2")
df_issuers_types = df_issuers_types.drop(columns = ['investment_interests'])

# Investors - bubbles
# Here i make the groupbys and rename the columns to have a table with the investment interests, Count and Percent of Investors with that. 
df_bubbles_count = df_investor_bubbles.groupby('investment_interests').count().user_id.reset_index()
df_bubbles_count.sort_values(by=['user_id'], inplace=True, ascending=False)
df_bubbles_count.rename(columns={'user_id':'Count'}, inplace=True)
df_bubbles_count.rename(columns={'investment_interests':'Investment Interests'}, inplace=True)
# adds a columns with the percentage
df_bubbles_count['Percent'] = ((df_bubbles_count['Count'] / total_tags) * 100).round()
df_bubbles_count['Percent'] = df_bubbles_count['Percent'].apply(lambda x: int(x))

# Issuers - bubbles
# Groupby for the bubbles or tags used also rename and sort by count. 
df_bubbles_count_issuers = df_issuers_bubbles.groupby('investment_interests').count().user_id.reset_index()
df_bubbles_count_issuers.sort_values(by=['user_id'], inplace=True, ascending=False)
df_bubbles_count_issuers.rename(columns={'user_id':'Count'}, inplace=True)
df_bubbles_count_issuers.rename(columns={'investment_interests':'Investment Interests'}, inplace=True)
# adds a columns with the percentage
df_bubbles_count_issuers['Percent'] = ((df_bubbles_count_issuers['Count'] / total_tags_issuers) * 100).round()
df_bubbles_count_issuers['Percent'] = df_bubbles_count_issuers['Percent'].apply(lambda x: int(x))

# This just takes that same table and reduces it to be the top 25 
df_bubbles_count_reduced = df_bubbles_count.head(25)
df_bubbles_count_reduced_issuers = df_bubbles_count_issuers.head(25)

# Investors - Types 
# Here do the same thing using groupby to get the investment types, count and percentage of Investors that selected each category 
df_types_count = df_investor_types.groupby('investment_answer_2').count().user_id.reset_index()
df_types_count.sort_values(by=['user_id'], inplace=True, ascending=False)
df_types_count.rename(columns={'user_id':'Count'}, inplace=True)
df_types_count.rename(columns={'investment_answer_2':'Investment Types'}, inplace=True)
# adds a columns with the percentage
df_types_count['Percent'] = ((df_types_count['Count'] / total_types) * 100).round()
df_types_count['Percent'] = df_types_count['Percent'].apply(lambda x: int(x))

# Issuers - Types
df_types_count_issuers = df_issuers_types.groupby('investment_answer_2').count().user_id.reset_index()
df_types_count_issuers.sort_values(by=['user_id'], inplace=True, ascending=False)
df_types_count_issuers.rename(columns={'user_id':'Count'}, inplace=True)
df_types_count_issuers.rename(columns={'investment_answer_2':'Investment Types'}, inplace=True)
# adds a columns with the percentage
df_types_count_issuers['Percent'] = ((df_types_count['Count'] / total_types_issuers) * 100).round()
df_types_count_issuers['Percent'] = df_types_count['Percent'].apply(lambda x: int(x))

# This section uses the Monthly_Breakdown Jupyter file to build the queries

# Investor and Issuer Monthly Breakdown Code 
# Querying from my SQl database to show all Investors that have profiles that haven't been deleted
df_investors_monthly = pd.read_sql("""SELECT DISTINCT user_id, email, full_name, account_type, public.users.invited_by_id, 
                                      public.user_accounts.deleted_at, public.user_accounts.created_at, investment_interests, investment_answer_2 
                                      FROM public.users JOIN public.user_accounts ON public.user_accounts.user_id = public.users.id 
                                      WHERE account_type LIKE 'Investor' AND public.user_accounts.deleted_at IS NULL 
                                      AND public.users.encrypted_password NOT LIKE '' 
                                      ORDER BY user_id ASC;""", con=cnx)

# Querying from my SQl database to show all Issuers with profiles filled out 
df_issuers_monthly = pd.read_sql("""SELECT DISTINCT user_id, email, full_name, account_type, public.users.invited_by_id, 
                                    public.user_accounts.deleted_at, public.user_accounts.created_at, investment_interests, investment_answer_2 
                                    FROM public.users JOIN public.user_accounts ON public.user_accounts.user_id = public.users.id 
                                    WHERE account_type LIKE 'IssuerAdmin' AND public.user_accounts.deleted_at IS NULL 
                                    AND public.users.encrypted_password NOT LIKE '' 
                                    ORDER BY user_id ASC;""", con=cnx)


# Deleting the values that repeat for investor accounts getting rid of the same name and user_id
df_investors_monthly.drop_duplicates(subset=["user_id"], keep="first", inplace=True)
df_investors_monthly.drop_duplicates(subset=["full_name"], keep="first", inplace=True)

# Deleting the values that repeat for issuer accounts getting rid of the same name and user_id
df_issuers_monthly.drop_duplicates(subset=["user_id"], keep="first", inplace=True)
df_issuers_monthly.drop_duplicates(subset=["full_name"], keep="first", inplace=True)

# Searching for values within the full names and emails to be able to remove the extra esther accounts and any test accounts 
search_values_monthly = ['test','Test','DELETE', 'Delete','estherkiim93', 'admin', 'johnbracken', 'Bill Militello', 
'Matt Nicklas', 'Laura Calazans', 'Binh Phan']

# Investors
df_removal_title_invest_monthly = df_investors_monthly[df_investors_monthly.full_name.str.contains('|'.join(search_values_monthly))]
df_removal_email_invest_monthly = df_investors_monthly[(df_investors_monthly.email.str.contains('|'.join(search_values_monthly)))]

# Issuers
df_removal_title_issuer_monthly = df_issuers_monthly[df_issuers_monthly.full_name.str.contains('|'.join(search_values_monthly))]
df_removal_email_issuer_monthly = df_issuers_monthly[(df_issuers_monthly.email.str.contains('|'.join(search_values_monthly)))]

# Investor
cond_invest_monthly = df_investors_monthly['full_name'].isin(df_removal_title_invest_monthly['full_name'])
cond_email_monthly = df_investors_monthly['email'].isin(df_removal_email_invest_monthly['email'])

# Issuer
cond_issuer_monthly = df_issuers_monthly['full_name'].isin(df_removal_title_issuer_monthly['full_name'])
cond_email_2_monthly = df_issuers_monthly['email'].isin(df_removal_email_issuer_monthly['email'])

# Drop the values from each of the dfs for Investors and Issuers
df_investors_monthly.drop(df_investors_monthly[cond_invest_monthly].index, inplace = True)
df_investors_monthly.drop(df_investors_monthly[cond_email_monthly].index, inplace = True)
df_issuers_monthly.drop(df_issuers_monthly[cond_issuer_monthly].index, inplace = True)
df_issuers_monthly.drop(df_issuers_monthly[cond_email_2_monthly].index, inplace = True)

# Investor - Time
# Here I split apart the date time values to have just the month and year as a column 
df_investors_monthly['Date'] = df_investors_monthly['created_at'].dt.strftime('%Y-%m') 
df_time_final_investor  = df_investors_monthly.groupby('Date').size().reset_index().rename(columns={0: 'Count_Signups'})
# This code takes the dates above 2019-12 and removes them creating a dataframe with all the relevant information. 
df_time_final_investor = df_time_final_investor[df_time_final_investor['Date'] > '2019-12']

# Issuer - Time
# Here I split apart the date time values to have just the month and year as a column 
df_issuers_monthly['Date'] = df_issuers_monthly['created_at'].dt.strftime('%Y-%m')
df_time_final_issuer  = df_issuers_monthly.groupby('Date').size().reset_index().rename(columns={0: 'Count_Signups'})
# This code takes the dates above 2019-12 and removes them creating a dataframe with all the relevant information. 
df_time_final_issuer = df_time_final_issuer[df_time_final_issuer['Date'] > '2019-12']

# Investor
# Want to make sure to add the total amount on each month as well using cum sum function
df_time_final_investor = df_time_final_investor.assign(Total_Investors = df_time_final_investor.Count_Signups.cumsum())

# Issuer
# Want to make sure to add the total amount on each month as well using cum sum function
df_time_final_issuer = df_time_final_issuer.assign(Total_Issuers = df_time_final_issuer.Count_Signups.cumsum())

# MOST ENGAGED USER SECTION 

def build_df(SQL_query):
    return pd.read_sql(SQL_query, con=cnx)

df_engaged = build_df("""WITH test_table AS (
                         SELECT u.id, u.full_name,ua.account_type FROM users u JOIN user_accounts ua ON u.id=ua.user_id 
                         WHERE account_type='IssuerAdmin'
                         UNION
                         SELECT u.id, u.full_name,ua.account_type FROM users u JOIN user_accounts ua ON u.id=ua.user_id WHERE u.id NOT IN
                         (SELECT u.id FROM users u JOIN user_accounts ua ON u.id=ua.user_id WHERE account_type='IssuerAdmin')
                                                                                                                            )
                         SELECT DISTINCT act.performer_id, u.full_name, u.email, act.type, act.created_at, u.sign_in_count, tt.account_type 
                         FROM activities act
                         JOIN users u ON act.performer_id = u.id
                         JOIN test_table tt ON u.id = tt.id
                         WHERE act.created_at >= '2021-01-01 00:00:00' AND act.created_at <= '2021-07-31 00:00:00' 
                         AND act.deleted_at IS NULL;""")

df_current_engaged = build_df("""WITH test_table AS (
                                 SELECT u.id, u.full_name,ua.account_type FROM users u JOIN user_accounts ua ON u.id=ua.user_id 
                                 WHERE account_type='IssuerAdmin'
                                 UNION
                                 SELECT u.id, u.full_name,ua.account_type FROM users u JOIN user_accounts ua ON u.id=ua.user_id 
                                 WHERE u.id NOT IN
                                 (SELECT u.id FROM users u JOIN user_accounts ua ON u.id=ua.user_id WHERE account_type='IssuerAdmin')
                                                                                                                                    )
                                 SELECT DISTINCT act.performer_id, u.full_name, u.email, act.type, act.created_at, u.sign_in_count, tt.account_type 
                                 FROM activities act
                                 JOIN users u ON act.performer_id = u.id
                                 JOIN test_table tt ON u.id = tt.id
                                 WHERE act.created_at >= '2021-07-01 00:00:00' AND act.created_at <= '2021-07-31 00:00:00' 
                                 AND act.deleted_at IS NULL;""")

# # Most Engaged - YEAR 
# # This is to be able to replace any user that has both an investor and issuer account to just have an issuer account. 
# # Using the duplicated method which outputs booleans
# for i, row in df_engaged.iterrows():
#     if df_engaged.duplicated(subset=["created_at"], keep=False)[i] == True:
#         user_engaged = 'IssuerAdmin'
#         df_engaged.at[i, 'account_type'] = user_engaged


# # MOST ENGAGED - CURRENT MONTH ( NEED TO UPDATE TO MAKE THE MONTH VALUE A VARIABLE THAT CHNAGES ACCORDINGLINGLY) 
# for i, row in df_current_engaged.iterrows():
#     if df_current_engaged.duplicated(subset=["created_at"], keep=False)[i] == True:
#         user = 'IssuerAdmin'
#         df_current_engaged.at[i, 'account_type'] = user

# # Attempt to make a function instead that will be able to accomplish the same thing and make my code a little faster maybe?
# @st.cache
# def replace_account_type(dataframe, replacement):
#     for i, row in dataframe.iterrows():
#         if dataframe.duplicated(subset=["created_at"], keep=False)[i] == True:
#             user = replacement
#             dataframe.at[i, 'account_type'] = user
    
# # Test it out applying my functions takes awhile and sometimes in makes the Issuers investors which it shouldn't do!!!!!!!!!!!
# replace_account_type(df_engaged, 'IssuerAdmin')
# replace_account_type(df_current_engaged, 'IssuerAdmin')

# Deleting the values that repeat for investor accounts 
df_engaged.drop_duplicates(subset=["created_at"], keep="last", inplace=True)
df_current_engaged.drop_duplicates(subset=["created_at"], keep="last", inplace=True)

# Searching for values within the full names and emails to be able to remove the extra esther accounts and any test accounts 
search_values_engaged = ['test','Test','DELETE', 'Delete','estherkiim93', 'admin', 'johnbracken', 'Bill Militello', 'Matt Nicklas',
                         'Binh Phan','Localvest Admin', 'Laura Calazans', 'Team Localvest', 'Amanda Dawson', 'Allison Johnson', 
                         'Drazen Alcocer', 'Esther Kim', 'John Bracken', 'Matthew Nicklas']

# Year
df_removal_name_engaged = df_engaged[df_engaged.full_name.str.contains('|'.join(search_values_engaged))]
df_removal_email_engaged = df_engaged[(df_engaged.email.str.contains('|'.join(search_values_engaged)))]

# Current
df_removal_name_current = df_current_engaged[df_current_engaged.full_name.str.contains('|'.join(search_values_engaged))]
df_removal_email_current = df_current_engaged[(df_current_engaged.email.str.contains('|'.join(search_values_engaged)))]

# Year
cond_engaged_name = df_engaged['full_name'].isin(df_removal_name_engaged['full_name'])
cond_engaged_email = df_engaged['email'].isin(df_removal_email_engaged['email'])

# Current
cond_current_name = df_current_engaged['full_name'].isin(df_removal_name_current['full_name'])
cond_current_email = df_current_engaged['email'].isin(df_removal_email_current['email'])

# Drop the values from each of the dfs for Year and Current Month
df_engaged.drop(df_engaged[cond_engaged_name].index, inplace = True)
df_engaged.drop(df_engaged[cond_engaged_email].index, inplace = True)
df_current_engaged.drop(df_current_engaged[cond_current_name].index, inplace = True)
df_current_engaged.drop(df_current_engaged[cond_current_email].index, inplace = True)

# Can just keep the stuff I want to base it off of instead of getting rid of what I don't want !!!!!!!!!!!!!
search_values_important_activity = ['ActivityType::InvestmentUpdate', 'ActivityType::InvestmentView', 'ActivityType::SignIn', 'ActivityType::CommunityEventAdded',
                                   'ActivityType::FeedLike', 'ActivityType::FeedUnlike', 'ActivityType::VideoView', 'ActivityType::CommentLike', 'ActivityType::CommentUnlike', 
                                    'ActivityType::CommentCreation', 'ActivityType::ConversationCreation', 'ActivityType::SelfAccountUpdate', 'ActivityType::InvestmentShareCreation',
                                   'ActivityType::UpdateShared']

# With the dropped values it seems to give me an error sometimes so will need to figure out why that is happening to me!!!!!!!!
df_most = df_engaged[df_engaged['type'].isin(search_values_important_activity)]
df_most_current = df_current_engaged[df_current_engaged['type'].isin(search_values_important_activity)]

# Year
# This groupby is trying to get the most engaged users by using certain things that I want to keep! 
df_most_engaged_new = df_most.groupby(['full_name', 'account_type']).count().type.reset_index()
df_most_engaged_new.sort_values(by=['type'], inplace=True, ascending=False)
df_most_engaged_new = df_most_engaged_new.reset_index(drop = True)
# Renaming the columns
df_most_engaged_new.rename(columns={'full_name':'Name'}, inplace=True)
df_most_engaged_new.rename(columns={'type':'Count'}, inplace=True)

# Current
# This groupby is not working properly need to figure out why this is happening and what it all means
df_most_engaged_new_current = df_most_current.groupby(['full_name', 'account_type']).count().type.reset_index()
df_most_engaged_new_current.sort_values(by=['type'], inplace=True, ascending=False)
df_most_engaged_new_current = df_most_engaged_new_current.reset_index(drop = True)
# Renaming the columns
df_most_engaged_new_current.rename(columns={'full_name':'Name'}, inplace=True)
df_most_engaged_new_current.rename(columns={'type':'Count'}, inplace=True)

# Top 10 Most Engaged Users
df_most_engaged_new_top_10 = df_most_engaged_new.head(20)

# Top 10 Most Engaged Users - Current Month
df_most_engaged_new_current_top_10 = df_most_engaged_new_current.head(20)

# Most engaged issuers and Investors 
df_most_engaged_issuer = df_most_engaged_new.loc[df_most_engaged_new['account_type'] == 'IssuerAdmin']
df_most_engaged_investor = df_most_engaged_new.loc[df_most_engaged_new['account_type'] == 'Investor']

# Most Engaged Issuers and Investors broken down for Current Month
df_most_engaged_issuer_current = df_most_engaged_new_current.loc[df_most_engaged_new_current['account_type'] == 'IssuerAdmin']
df_most_engaged_investor_current = df_most_engaged_new_current.loc[df_most_engaged_new_current['account_type'] == 'Investor']

# Top 10 for 2021
df_most_engaged_issuer = df_most_engaged_issuer.head(20)
df_most_engaged_investor = df_most_engaged_investor.head(20)

# Top 10 for Current Month
df_most_engaged_issuer_current = df_most_engaged_issuer_current.head(20)
df_most_engaged_investor_current = df_most_engaged_investor_current.head(20)


# Next get Most Active Users based on Sign ins Alone.  (NEW SECTION)
#Need to put a sql query here so that I can get what I need and adjust the date 

df_sign_in = df_engaged[df_engaged['type']=='ActivityType::SignIn']
df_sign_in_current = df_current_engaged[df_current_engaged['type']=='ActivityType::SignIn']

# Year - Sign in
# This groupby is not working properly need to figure out why this is happening and what it all means
df_most_active = df_sign_in.groupby(['full_name', 'account_type']).count().type.reset_index()
df_most_active.sort_values(by=['type'], inplace=True, ascending=False)
df_most_active = df_most_active.reset_index(drop=True)
# Renaming the columns
df_most_active.rename(columns={'full_name':'Name'}, inplace=True)
df_most_active.rename(columns={'type':'Count'}, inplace=True)

# Current - Sign in
# This groupby is not working properly need to figure out why this is happening and what it all means
df_most_active_current = df_sign_in_current.groupby(['full_name', 'account_type']).count().type.reset_index()
df_most_active_current.sort_values(by=['type'], inplace=True, ascending=False)
df_most_active_current = df_most_active_current.reset_index(drop=True)
# Renaming the columns
df_most_active_current.rename(columns={'full_name':'Name'}, inplace=True)
df_most_active_current.rename(columns={'type':'Count'}, inplace=True)

# Top 10 Most Engaged Users - Sign in only
df_most_active = df_most_active.head(20)

# Top 10 Most Engaged Users - Current Month
df_most_active_current = df_most_active_current.head(20)

# Most engaged issuers and Investors - sign in only
df_most_active_issuer = df_most_active.loc[df_most_active['account_type'] == 'IssuerAdmin']
df_most_active_investor = df_most_active.loc[df_most_active['account_type'] == 'Investor']

# Most Engaged Issuers and Investors broken down for Current Month
df_most_active_current_issuer = df_most_active_current.loc[df_most_active_current['account_type'] == 'IssuerAdmin']
df_most_active_current_investor = df_most_active_current.loc[df_most_active_current['account_type'] == 'Investor']

# Top 10 for 2021 - sign in only
df_most_active_issuer = df_most_engaged_issuer.head(20)
df_most_active_investor = df_most_engaged_investor.head(20)

# Top 10 for Current Month
df_most_active_current_issuer = df_most_active_current_issuer.head(20)
df_most_active_current_investor = df_most_active_current_investor.head(20)




# Most Active Looking at how many users fit this category

df_active = df_engaged[df_engaged['type']=='ActivityType::SignIn']

# Here I split apart the date time values to have just the month and year as a column 
df_active['Date'] = df_active['created_at'].dt.strftime('%Y-%m')

# Drop duplicates to get users who have signed in each month but elimnate each sign in line from the original table 
df_active.drop_duplicates(subset=["full_name", "Date"], keep="last", inplace=True)

# Try this code out in order to havea column with just the month and year to groupby which will be super helpful when trying to graph this stuff. 
df_active_time  = df_active.groupby(['Date']).size().reset_index().rename(columns={0: 'Active_Users'})
df_active_time = df_active_time[df_active_time['Date'] > '2019-12']

# INVESTORS - ACTIVE
df_active_investor = df_active[df_active['account_type']=='Investor']
# Here I split apart the date time values to have just the month and year as a column 
df_active_investor['Date'] = df_active_investor['created_at'].dt.strftime('%Y-%m')

# Drop duplicates to get users who have signed in each month but elimnate each sign in line from the original table 
df_active_investor.drop_duplicates(subset=["full_name", "Date"], keep="last", inplace=True)

# Try this code out in order to havea column with just the month and year to groupby which will be super helpful when trying to graph this stuff. 
df_active_investor_time  = df_active_investor.groupby(['Date']).size().reset_index().rename(columns={0: 'Active_Users'})
df_active_investor_time = df_active_investor_time[df_active_investor_time['Date'] > '2019-12']

# ISSUERS - ACTIVE
df_active_issuer = df_active[df_active['account_type']=='IssuerAdmin']
# Here I split apart the date time values to have just the month and year as a column 
df_active_issuer['Date'] = df_active_issuer['created_at'].dt.strftime('%Y-%m')

# Drop duplicates to get users who have signed in each month but elimnate each sign in line from the original table 
df_active_issuer.drop_duplicates(subset=["full_name", "Date"], keep="last", inplace=True)

# Try this code out in order to havea column with just the month and year to groupby which will be super helpful when trying to graph this stuff. 
df_active_issuer_time  = df_active_issuer.groupby(['Date']).size().reset_index().rename(columns={0: 'Active_Users'}) 
df_active_issuer_time = df_active_issuer_time[df_active_issuer_time['Date'] > '2019-12']


# # Need to get a df with time from the beginning of the app so I can combine with most active to be able to compare 

# # Investor - Time
# df_time_final_investor_active  = df_investors_monthly.groupby('Date').size().reset_index().rename(columns={0: 'Count_Signups'})
# # This code takes the dates above 2019-12 and removes them creating a dataframe with all the relevant information. 
# df_time_final_investor_active = df_time_final_investor_active[df_time_final_investor_active['Date'] > '2010-12']

# # Issuer - Time
# df_time_final_issuer_active  = df_issuers_monthly.groupby('Date').size().reset_index().rename(columns={0: 'Count_Signups'})
# # This code takes the dates above 2019-12 and removes them creating a dataframe with all the relevant information. 
# df_time_final_issuer_active = df_time_final_issuer_active[df_time_final_issuer_active['Date'] > '2010-12']

# # Investor
# # Want to make sure to add the total amount on each month as well using cum sum function
# df_time_final_investor_active = df_time_final_investor_active.assign(Total_Investors = df_time_final_investor_active.Count_Signups.cumsum())

# # Issuer
# # Want to make sure to add the total amount on each month as well using cum sum function
# df_time_final_issuer_active = df_time_final_issuer_active.assign(Total_Issuers = df_time_final_issuer_active.Count_Signups.cumsum())

# df_time_final_investor_active_tail = df_time_final_investor_active.tail(8)
# df_time_final_issuer_active_tail = df_time_final_issuer_active.tail(8)

# # EXPERKMIENTLKHETL:KJFLKDJF:LSDKJFLKSJDFLKJSDL:KFJS:LDKFJ:LSKDJ
# df_active_investor_time['Date'] = df_active_investor_time['Date'].apply(lambda x: str(x))
# df_time_final_investor_active_tail['Date'] = df_time_final_investor_active_tail['Date'].apply(lambda x: str(x))

# df_active_investor_time_com = df_active_investor_time.join(df_time_final_investor_active_tail['Total_Investors'])
# df_active_investor_time_com

# THIS IS TEMPORARY BUT IT WORKS
list_of_invest = [1229, 1266, 1313, 1390, 1436, 1480, 1516]
df_active_investor_time['Total_Investors'] = list_of_invest

list_of_issuer = [324, 326, 335, 346, 356, 371, 379]
df_active_issuer_time['Total_Issuers'] = list_of_issuer













# Next Section creates the graphs with Altair
# THis is the Investor and Issuer Interests Section
# Investor - Count
# Here is making a bar chart using Altair methods and then displahing through streamlit. 
chart = (getattr(alt.Chart(df_bubbles_count_reduced),"mark_" + "bar")()
          .encode(alt.X("Count", axis=alt.Axis(title='Count', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Interests", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Interests", "Count"]))
          .interactive())

# Investor - Percentage 
chart_percentage = (getattr(alt.Chart(df_bubbles_count_reduced),"mark_" + "bar")()
          .encode(alt.X("Percent", axis=alt.Axis(title='Percent', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Interests", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Interests", "Percent"]))
          .interactive())

# Issuer - Count
# Here is making a bar chart using Altair methods and then displahing through streamlit. 
chart_issuer = (getattr(alt.Chart(df_bubbles_count_reduced_issuers),"mark_" + "bar")()
          .encode(alt.X("Count", axis=alt.Axis(title='Count', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Interests", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Interests", "Count"]))
          .interactive())

# Issuer - Percentage
chart_issuer_percentage = (getattr(alt.Chart(df_bubbles_count_reduced_issuers),"mark_" + "bar")()
          .encode(alt.X("Percent", axis=alt.Axis(title='Percent', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Interests", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Interests", "Percent"]))
          .interactive())

# This sections takes the investment type and graphs using ALtair like above

# Investor - Count
chart_type = (getattr(alt.Chart(df_types_count),"mark_" + "bar")()
          .encode(alt.X("Count", axis=alt.Axis(title='Count', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Types", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Types", "Count"]))
          .interactive())

# Investor - Percentage 
chart_type_percentage = (getattr(alt.Chart(df_types_count),"mark_" + "bar")()
          .encode(alt.X("Percent", axis=alt.Axis(title='Percent', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Types", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Types", "Percent"]))
          .interactive())

# Issuer - Count
# Here is making a bar chart using Altair methods and then displahing through streamlit. 
chart_issuer_type = (getattr(alt.Chart(df_types_count_issuers),"mark_" + "bar")()
          .encode(alt.X("Count", axis=alt.Axis(title='Count', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Types", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Types", "Count"]))
          .interactive())

# Issuer - Percentage
chart_issuer_percentage_type = (getattr(alt.Chart(df_types_count_issuers),"mark_" + "bar")()
          .encode(alt.X("Percent", axis=alt.Axis(title='Percent', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Investment Types", axis=alt.Axis(title='Investment Interests', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Investment Types", "Percent"]))
          .interactive())

# This is for the Monthly breakdown since Jan 2020 for Investors and Issuers 

# Investors 
monthly_chart_base = alt.Chart(df_time_final_investor, height = 600).encode(
            alt.X("Date", axis=alt.Axis(title='Date (Month)', titleFontSize=20, labelFontSize=15)))

monthly_chart_bar = monthly_chart_base.mark_bar().encode(
            alt.Y("Count_Signups", sort="x", axis=alt.Axis(title='Investor Accounts (Count)', titleFontSize=20, labelFontSize=15)), 
            alt.Tooltip(["Date", "Count_Signups"]))
          
monthly_chart_line = monthly_chart_base.mark_line(color='#F54F0C',strokeWidth=7).encode(
            alt.Y("Total_Investors", sort="x", axis=alt.Axis(title='Total Count', titleFontSize=20, labelFontSize=15)), 
            alt.Tooltip(["Date", "Total_Investors"]))

monthly_chart_point = monthly_chart_base.mark_point(color='#F54F0C',strokeWidth=7, opacity=1).encode(
            alt.Y("Total_Investors", sort="x", axis=alt.Axis(title='Total Count', titleFontSize=20, labelFontSize=15)),
            alt.Tooltip(["Date", "Total_Investors"]))

# Issuers 
monthly_chart_base_Issuer = alt.Chart(df_time_final_issuer, height = 600).encode(
            alt.X("Date", axis=alt.Axis(title='Date (Month)', titleFontSize=20, labelFontSize=15)))

monthly_chart_bar_Issuer = monthly_chart_base_Issuer.mark_bar().encode(
            alt.Y("Count_Signups", sort="x", axis=alt.Axis(title='Issuer Accounts (Count)', titleFontSize=20, labelFontSize=15)), 
            alt.Tooltip(["Date", "Count_Signups"]))
          
monthly_chart_line_Issuer = monthly_chart_base_Issuer.mark_line(color='#F54F0C',strokeWidth=7).encode(
            alt.Y("Total_Issuers", sort="x", axis=alt.Axis(title='Total Count', titleFontSize=20, labelFontSize=15)), 
            alt.Tooltip(["Date", "Total_Issuers"]))

monthly_chart_point_Issuer = monthly_chart_base_Issuer.mark_point(color='#F54F0C',strokeWidth=7, opacity=1).encode(
            alt.Y("Total_Issuers", sort="x", axis=alt.Axis(title='Total Count', titleFontSize=20, labelFontSize=15)),
            alt.Tooltip(["Date", "Total_Issuers"]))

# Active User Graphs

# Maybe I can do a layered bar chart for this somehow showing the percentage of those that are active versus inactive????
# Do same code I do above for monthly breakdown and merge with this table so I can compare the two bar graphs as one happy family. 
# Active User
chart_active_user = (getattr(alt.Chart(df_active_time),"mark_" + "bar")()
          .encode(alt.X("Date", axis=alt.Axis(title='Date', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Active_Users", axis=alt.Axis(title='Count of Active Users', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Active_Users", "Date"]))
          .interactive())

chart_active_user_investor = (getattr(alt.Chart(df_active_investor_time),"mark_" + "bar")()
          .encode(alt.X("Date", axis=alt.Axis(title='Date', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Active_Users", axis=alt.Axis(title='Count of Active Users', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Active_Users", "Date"]))
          .interactive())

chart_active_user_issuer = (getattr(alt.Chart(df_active_issuer_time),"mark_" + "bar")()
          .encode(alt.X("Date", axis=alt.Axis(title='Date', titleFontSize=20, labelFontSize=15)), 
                  alt.Y("Active_Users", axis=alt.Axis(title='Count of Active Users', titleFontSize=20, labelFontSize=15), sort="-x"), 
                  alt.Tooltip(["Active_Users", "Date"]))
          .interactive())

# Active Investors compared to total
active_users_base = alt.Chart(df_active_investor_time, height = 600).encode(
            alt.X("Date", axis=alt.Axis(title='Date (Month)', titleFontSize=20, labelFontSize=15)))

active_users_bar = active_users_base.mark_bar(color='#F54F0C').encode(
            alt.Y("Active_Users", sort="x", axis=alt.Axis(labelFontSize=15)), 
            alt.Tooltip(["Date", "Active_Users"]))

active_users_bar_total = active_users_base.mark_bar().encode(
            alt.Y("Total_Investors", sort="x", axis=alt.Axis(title='Total Registered Investors', titleFontSize=20, labelFontSize=15)), 
            alt.Tooltip(["Date", "Total_Investors"]))

# Active Issuers compared to total
active_users_base_issuer = alt.Chart(df_active_issuer_time, height = 600).encode(
            alt.X("Date", axis=alt.Axis(title='Date (Month)', titleFontSize=20, labelFontSize=15)))

active_users_bar_issuer = active_users_base_issuer.mark_bar(color='#F54F0C').encode(
            alt.Y("Active_Users", sort="x", axis=alt.Axis(labelFontSize=15)), 
            alt.Tooltip(["Date", "Active_Users"]))

active_users_bar_total_issuer = active_users_base_issuer.mark_bar().encode(
            alt.Y("Total_Issuers", sort="x", axis=alt.Axis(title='Total Registered Issuers', titleFontSize=20, labelFontSize=15)), 
            alt.Tooltip(["Date", "Total_Issuers"]))

# st.altair_chart(alt.layer(active_users_bar_total, active_users_bar), use_container_width= True)
# st.altair_chart(alt.layer(active_users_bar_total_issuer, active_users_bar_issuer), use_container_width= True)










# Here is the Streamlit Visualization work 

# SETTING PAGE CONFIG TO WIDE MODE
#st.set_page_config(layout="wide")

# st.markdown("""
# <style>
# .big-font {
#     font-size:300px !important;
# }
# </style>
# """, unsafe_allow_html=True)

# st.markdown('<p class="big-font">Hello World !!</p>', unsafe_allow_html=True)

# Title for the website
st.title('Localvest Dashboard - Matt Nicklas')

# Sidebar
st.sidebar.title('Navigation Panel')
section_dropdown = st.sidebar.selectbox("User", ["Investors", "Issuers", "Monthly", "Most Engaged Users"], key = 'dropdown')

# Here are the Final charts we will have to keep working on these over time 

# Investor
if section_dropdown == 'Investors':
    # Investor Interests
    st.header('Investor Interests Information')
    occupation = st.selectbox("Select Count or Percent", ["Count", 'Percent'], key = 'Investor')
    if occupation == "Count":
        st.subheader('Number of Investors Interested in Top 25 Interest Tags')
        st.altair_chart(chart.properties(width=1400, height = 600))
    else:
        st.subheader('Percent of Investors Interested in Top 25 Interest Tags')
        st.altair_chart(chart_percentage.properties(width=1400, height = 600))

    # Investor Types
    st.header('Investor Types Information')
    occupation_type = st.selectbox("Select Count or Percent", ["Count", 'Percent'], key = 'Investor_type')
    if occupation_type == "Count":
        st.subheader('Amount of Investors Selecting Each Investment Type')
        st.altair_chart(chart_type.properties(width=1400, height = 600))
    else:
        st.subheader('Percent of Investors Selecting Each Investment Type')
        st.altair_chart(chart_type_percentage.properties(width=1400, height = 600))
    if st.checkbox('Show Investor Data', key = 'Percent_type'):
        st.subheader('Investor Data')
        st.write(df_investors)

    # Investor ACTIVE USERS TEST GRAPH NEED TO ADJUST 
    st.header('Active Investors')
    st.subheader('Active Investors by Month Comapared to Total # of Registered Investors')
    st.altair_chart(alt.layer(active_users_bar_total, active_users_bar), use_container_width= True)
    if st.checkbox('Show Investor Data', key = 'active_1'):
        st.subheader('Investor Data')
        st.write(df_active_investor_time)

# Issuer
if section_dropdown == 'Issuers':
    # Issuer Interests
    st.header('Issuer Interests Information')
    occupation_2 = st.selectbox("Select Count or Percent", ["Count", 'Percent'], key = 'Issuer')
    if occupation_2 == "Count":
        st.subheader('Number of Issuers Interested in Top 25 Interest Tags')
        st.altair_chart(chart_issuer.properties(width=1400, height = 600))
    else:
        st.subheader('Percent of Issuers Interested in Top 25 Interest Tags')
        st.altair_chart(chart_issuer_percentage.properties(width=1400, height = 600))

    # Issuer - types
    st.header('Issuer Types Information')
    occupation_type_2 = st.selectbox("Select Count or Percent", ["Count", 'Percent'], key = 'Issuer_type')
    if occupation_type_2 == "Count":
        st.subheader('Amount of Issuers Selecting Each Investment Type')
        st.altair_chart(chart_issuer_type.properties(width=1400, height = 600))
    else:
        st.subheader('Percent of Issuers Selecting Each Investment Type')
        st.altair_chart(chart_issuer_percentage_type.properties(width=1400, height = 600))
    if st.checkbox('Show Issuer Data', key = 'Percent_Issuer_type'):
        st.subheader('Issuer Data')
        st.write(df_issuers)

    # Issuer ACTIVE USERS TEST GRAPH NEED TO ADJUST
    st.header('Active Issuers')
    st.subheader('Active Issuers by Month Comapared to Total # of Registered Issuers')
    st.altair_chart(alt.layer(active_users_bar_total_issuer, active_users_bar_issuer), use_container_width= True)
    if st.checkbox('Show Issuer Data', key = 'active_1'):
        st.subheader('Issuer Data')
        st.write(df_active_issuer_time)

# Monthly Breakdown Charts!!!!!
# Investor
if section_dropdown == 'Monthly':
    st.title('Monthly Breakdown For Users ')

    st.header('Number of New Investor Account Signups by Month Since Jan 2020')
    st.altair_chart(alt.layer(monthly_chart_bar, monthly_chart_line, monthly_chart_point).resolve_scale(y = 'independent'), use_container_width= True)
    if st.checkbox('Show Investor Data', key = 'monthly_breakdown'):
        st.subheader('Investor Data')
        st.write(df_time_final_investor)

# Issuer
    st.header('Number of New Issuer Account Signups by Month Since Jan 2020')
    st.altair_chart(alt.layer(monthly_chart_bar_Issuer, monthly_chart_line_Issuer, monthly_chart_point_Issuer)
                             .resolve_scale(y = 'independent'), use_container_width= True)
    if st.checkbox('Show Issuer Data', key = 'monthly'):
        st.subheader('Issuer Data')
        st.write(df_time_final_issuer)

# Most Engaged Users
if section_dropdown == 'Most Engaged Users':
    engaged_dropdown = st.selectbox("Engaged User Type", ["Active", "Super Active", "Profiles"], key = 'drops')

    # # This uses the criteria I put in place like investment views and everything to take this count
    # if engaged_dropdown == "Super Active":

    #     st.header('Most Engaged Users')
    #     st.subheader('Summary: This count shows most engaged users based on investment views and updates')
    #     col1, col2 = st.beta_columns(2)
    #     col1.header('Top 20 Engaged Users 2021')
    #     col1.table(df_most_engaged_new_top_10)
    #     col1.header('Top 20 Issuers 2021')
    #     col1.table(df_most_engaged_issuer) 
    #     col1.header('Top 20 Investors 2021')
    #     col1.table(df_most_engaged_investor)
    
    #     col2.header('Top 20 Engaged Users July')
    #     col2.table(df_most_engaged_new_current_top_10)
    #     col2.header('Top 20 Issuers July')
    #     col2.table(df_most_engaged_issuer_current)
    #     col2.header('Top 20 Investors July')
    #     col2.table(df_most_engaged_investor_current)
    
    # This section shows those users that have signed in the most 
    if engaged_dropdown == "Active":

        st.header('Most Active Users')
        st.subheader('Summary: This shows most engaged user based on number of sign ins alone')
        col1, col2 = st.beta_columns(2)
        col1.header('Top 20 Active Users 2021')
        col1.table(df_most_active)
        col1.header('Top 20 Issuers 2021')
        col1.table(df_most_active_issuer) 
        col1.header('Top 20 Investors 2021')
        col1.table(df_most_active_investor)
    
        col2.header('Top 20 Active Users July')
        col2.table(df_most_active_current)
        col2.header('Top 20 Issuers July')
        col2.table(df_most_active_current_issuer)
        col2.header('Top 20 Investors July')
        col2.table(df_most_active_current_investor)

    # Next Section will have stats for the Profiles and their Completion 
    if engaged_dropdown == "Profiles":

        # Investors
        st.title('INVESTOR PROFILES')
        st.write('Registered Investor Accounts = ' + str(df_investors_total))
        st.write('Complete Investor Profiles = ' + str(total_complete_profiles_investor))
        st.write('Investors who Filled out Tags = ' + str(total_tags))
        st.write('Investors who Filled out Asset Class = ' + str(total_types))
        st.write(str(investor_percent) + ' of profiles finished')

        #Issuers
        st.title('ISSUER PROFILES')
        st.write('Registered Issuer Accounts = ' + str(df_issuers_total))
        st.write('Complete Issuer Profiles = ' + str(total_complete_profiles_issuer))
        st.write('Issuers who Filled out Tags = ' + str(total_tags_issuers))
        st.write('Issuers who Filled out Asset Class = ' + str(total_types_issuers))
        st.write(str(issuer_percent) + ' of profiles finished')

        # Who is updating their profile and could this be an eventual category here of some kind 







# Could have this but then do the Investment types as a section and make sure to add on the sidebar to select which stuff you want 

# Can do something with datetime value where I take it and then it will display for you how many views a tile has or someting like that 

# Could just move the checkbox for showing the data somewhere else and also add in lots more columns would be better for people to look at.
# Can clean that up and work from there to get a better representation of how I want things done. 