# Setup a database with the appropriate fields
# ingest the sourcedb into python
# clean the data - fillna, separate the city dict into fields
# setup a way to check for new/modified rows
# create target db
#write processed dataframe into targetdb
# send the processed data into the database
# write a sql query to transform the data.
# Automate reporting on the data. 

import pandas as pd
import pymysql.cursors
import os
import numpy as np
import re
import uszipcode
connection = pymysql.connect(host='localhost',
                             user=os.environ['MYSQLUID'],
                             password=os.environ['MYSQLPWD'],
                             database='sourcedb',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
sql = "SELECT * FROM user_table ORDER BY `inserted_at`"
df = pd.read_sql(sql, connection)

def preprocess_address():
    df.stripped_address = df.address.str.strip('{''}')
    df[["City", "State", "Country", "Postcode"]] = df["stripped_address"].str.split(",", n=3, expand= True)

preprocess_address()


def clean_address(col, to_strip):
    cleaned_value= col.replace(to_replace= to_strip,value='', regex= True)
    cleaned_value= cleaned_value.str.strip("' '")
    return cleaned_value
​
​

def clean_country():
    df.Country = clean_address(df.Country, "'country':")
    return df.Country
clean_country()


def clean_state():
    df.State = clean_address(df.State, "'state':")
    return df.State
clean_state()


def clean_city():
    df.City = clean_address(df.City, "'city':")
    return df.City
clean_city()

def clean_postcode():
    df.Postcode = clean_address(df.Postcode, "'postCode':")
    return df.Postcode
clean_postcode()

def fillNanVals(col,replace_with):
    if df[col].isna().sum() > 0:
        df[col].fillna(replace_with, inplace= True)
    
    else:
        pass


def fillNanIds():
    fill_null_with= 0
    fillNanVals("id",fill_null_with)
    return df.id
fillNanIds()

def fillNanCity():
    fillNanVals("City", "Gotham")
    return df.City

fillNanCity()

def fillNanState():
    fillNanVals("State","New Jersey")
    return df.State

fillNanState()


def fillNanCountry():
    fillNanVals("Country", "America")
    return df.Country

fillNanCountry()


def fillNanPostcode():
    fillNanVals("Postcode","53540")
    return df.Postcode

fillNanPostcode()


engine = create_engine('sqlite://', echo=False)

my_engine= engine.raw_connection()

target_data= pd.read_sql(sql, my_engine)
#check if state and postcode are the same
check_state=target_data[target_data['State']==target_data['Postcode']]
def fix_if_state_equals_postcode():
   if len(check_state)>0:
        for index, row in check_state.iterrows():
            if re.search ('[0-9]',row['State']):
               check_state['State']= search.by_zipcode(row['Postcode']).state
            else:
                for zipcode in search.by_city_and_state(city=row['City'],state=row['State']):
                     check_state['Postcode'] =zipcode.zipcode
        
        
target_data.update(check_state)

fix_if_state_equals_postcode()

#keep track of changes
track_changes= df
# Get rows not present in the target_data
track_changes[['changed_id','changed_inserted_at','changed_address']] = np.where(df[['id', 'inserted_at','address']] == target_data[['id', 'inserted_at','address']], True, False) 
track_changes

# Get modified rows
modified_rows=track_changes.loc[(track_changes['changed_id'] == False) | (track_changes['changed_inserted_at']== False)|track_changes['changed_address']==False]
modified_rows
#Get new records
inserts = track_changes[~track_changes.id.isin(target_data.id)]
inserts
#write initial data into the table
def extract_and_load(): 
    try:
        target_data.to_sql('user_etl_job', my_engine, if_exists='fail', index=False)
    except:
        print("This table already loaded")
extract_and_load()
#update sql if the source db has been modified
def update_to_sql():
    
      if len(modified_rows)>0:
         
        target_data.update(modified_rows)
        target_data.to_sql('user_etl_job',my_engine,if_exists='replace', index=False)
update_to_sql()

def insert_new_rows():
    if len(inserts)> 0:
        new_inserts = inserts.drop(['changed_city','changed_id', 'changed_inserted_at'], axis=1)
        target_data.append(new_inserts, ignore_index=True)
        target_data.to_sql('user_etl_job',my_engine,if_exists='append', index=False)
insert_new_rows()

def generate_report():
    sql= "SELECT `State`, COUNT(`id`)as Signups FROM user_etl_job GROUP BY `State` ORDER BY Signups DESC"
    report = pd.read_sql(sql, my_engine)
    report.to_csv('my report', header=['State', 'Total signups'], index=False)
generate_report()