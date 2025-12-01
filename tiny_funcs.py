import random
import pandas as pd
import numpy as np
from load_into_db import ENGINE


MONTHS = {
    # January
    'Jan': '01', 'JAN': '01', 'January': '01',
    # February  
    'Feb': '02', 'FEB': '02', 'February': '02',
    # March
    'Mar': '03', 'MAR': '03', 'March': '03',
    # April
    'Apr': '04', 'APR': '04', 'April': '04',
    # May
    'May': '05', 'MAY': '05', 'May': '05',
    # June
    'Jun': '06', 'JUN': '06', 'June': '06',
    # July
    'Jul': '07', 'JUL': '07', 'July': '07',
    # August
    'Aug': '08', 'AUG': '08', 'August': '08',
    # September
    'Sep': '09', 'SEP': '09', 'September': '09',
    # October
    'Oct': '10', 'OCT': '10', 'October': '10',
    # November
    'Nov': '11', 'NOV': '11', 'November': '11',
    # December
    'Dec': '12', 'DEC': '12', 'December': '12'
}

def getUsers():
    df = pd.read_sql('SELECT * FROM users', ENGINE)
    df = df.rename(columns={'id': 'id_user'})
    return df 

def fillShippingInfo(df):
    df_user = getUsers()
    df = df.merge(
        df_user[['id_user', 'address']],
        left_on='user_id',
        right_on='id_user',
        how='left'
    )
    df['shipping'] = df['address']
    return df['shipping']

def combineIds(df, group1, group2, group3, group4):
    df['duplicated_user_ids'] = df[group1 + "_ids"] + "," + df[group2 + "_ids"] + "," + df[group3 + "_ids"] + "," + df[group4 + "_ids"]
    df['duplicated_user_ids'] = (df['duplicated_user_ids']
                                 .str.replace(',', ' ', regex=True)
                                 .str.replace(r'\s+', ' ', regex=True)
                                 .str.strip()
                                 .str.replace(' ', ',', regex=True))
    return df['duplicated_user_ids']

def uniqueUsers(df):
    df = df.drop_duplicates(subset=['name', 'address', 'phone'], keep='first')
    df = df.drop_duplicates(subset=['name', 'address', 'email'], keep='first')
    df = df.drop_duplicates(subset=['name', 'phone', 'email'], keep='first')
    df = df.drop_duplicates(subset=['address', 'phone', 'email'], keep='first')
    return len(df) 

def removeSelfId(row, group_key):
    if len(row[group_key]) > 1:
        ids = row[group_key].split(',')
        ids.remove(str(row['id']))
        return ','.join(ids)
    return '' 

def idsByGroup(df, search_group):
    df['group_key'] = df[search_group.split(" ")].apply(tuple, axis=1)
    df[search_group] = df.groupby('group_key')['id'].transform(
        lambda user_id: ','.join(user_id.astype(str)) 
    )
    return df 

def findIds(df, search_group):
    df = idsByGroup(df, search_group)
    df[search_group + "_ids"] = df.apply(
        lambda row: removeSelfId(row, group_key=search_group),
        axis=1
    )
    df = df.drop(['group_key', search_group], axis=1)
    return df

def reprocessPrice(df):
    df['unit_price'] = (df['unit_price']
                        .str.replace(r'\s+', '', regex=True)
                        .str.replace(r'USD', '$', regex=True)
                        .str.replace(r'EUR', '€', regex=True)
                        .str.replace(r'\.$', '', regex=True)
                        .str.replace(r'(\d+)\$(\d+)¢', r'$\1.\2', regex=True)
                        .str.replace(r'(\d+)€(\d+)¢', r'€\1.\2', regex=True)
                        .str.replace(r'\¢', '.', regex=True)
                        .str.replace(r'(\d+\.?\d*)(\$)', r'\2\1', regex=True)
                        .str.replace(r'(\d+\.?\d*)(€)', r'\2\1', regex=True))
    return df['unit_price']

def addCurrencyType(df):
    df['currency_type'] = np.where(
        df['unit_price'].str.startswith("$"), 'USD', 'EUR'
    )
    return df['currency_type']

def convertEURtoUSD(df):
    df['unit_price'] = pd.to_numeric(df['unit_price'].str.replace(r'[$\€]', '', regex=True))
    df['unit_price'] = np.where(
        df['currency_type'] == 'EUR',
        (df['unit_price'] * 1.2).round(2),
        df['unit_price'] 
    )
    df['paid_price'] = (df['unit_price'] * df['quantity']).round(2)
    return df['unit_price']

def reprocessYear(df):
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    median_year = df['year'].median()
    df['year'] = df['year'].fillna(median_year).astype(int)
    df['year'] = df['year'].replace(0, median_year)
    return df['year']

def reprocessTitle(df):
    df['title'] = (df['title']
                   .str.replace(r"'(\w+)'", r"\1", regex=True)
                   .str.replace(r"''", r"'", regex=True)
                   .str.replace(r'–', r'-', regex=True))
    return df['title']

def authorSet(df):
    df['author_set'] = df['author'].str.split(", ").apply(lambda author_set: tuple(sorted(author_set)))
    return len(df['author_set'].value_counts())

def reprocessAuthor(df):
    df['author'] = (df['author']
                   .str.replace(r'\s+', r' ', regex=True)
                   .str.strip())
    return df['author']

def reprocessPublisher(df):
    most_common_publisher = df['publisher'].mode()[random.randint(0, 1)]
    df['publisher'] = df['publisher'].replace([" ", "", "NULL"], most_common_publisher)
    return df['publisher']

def reprocessPhone(df):
    df['phone'] = (df['phone']
                   .str.replace(r'[\)\.\s+]', '-', regex=True)
                   .str.replace(r'\((\d+)--', r'\1-', regex=True))
    return df['phone']