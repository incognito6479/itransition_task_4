import pandas as pd 
import yaml, re
import numpy as np
from tiny_funcs import reprocessYear, reprocessTitle, reprocessAuthor, \
reprocessPublisher, reprocessPhone, reprocessPrice, addCurrencyType, \
convertEURtoUSD, authorSet, findIds, uniqueUsers, combineIds, fillShippingInfo, MONTHS
from load_into_db import loadIntoDB


def reprocessBooksData(data_source):
    with open(f"DATAs/{data_source}/books.yaml", "r") as file:
        yaml_data = re.sub(r':(\w+):', r'\1:', file.read())
        yaml_data = yaml.safe_load(yaml_data)
    df = pd.DataFrame(yaml_data)
    df['year'] = reprocessYear(df)
    df['title'] = reprocessTitle(df)
    df['author'] = reprocessAuthor(df)
    df['publisher'] = reprocessPublisher(df)
    df['data_source'] = data_source
    df['author_set'] = authorSet(df)
    unique_author_set = len(df['author_set'].value_counts()) 
    df = df[['id', 'title', 'author', 'genre', 'publisher', 'year', 'data_source']]
    # loadIntoDB(df, 'books')

def reprocessUsersData(data_source):
    df = pd.read_csv(f"DATAs/{data_source}/users.csv")
    df['phone'] = reprocessPhone(df)
    group1 = " ".join(['name', 'address', 'phone'])
    group2 = " ".join(['name', 'address', 'email'])
    group3 = " ".join(['name', 'phone', 'email'])
    group4 = " ".join(['address', 'phone', 'email'])
    df = findIds(df, group1)  
    df = findIds(df, group2)
    df = findIds(df, group3)  
    df = findIds(df, group4)  
    df['duplicated_user_ids'] = combineIds(df, group1, group2, group3, group4)
    df['data_source'] = data_source 
    df = df[['id', 'name', 'address', 'phone', 'email', 'duplicated_user_ids', 'data_source']]
    loadIntoDB(df, 'users') 
    unique_users = uniqueUsers(df)
    print(unique_users)

def reprocessOrdersData(data_source):
    df = pd.read_parquet(f"DATAs/{data_source}/orders.parquet", engine="pyarrow")
    df['unit_price'] = reprocessPrice(df)
    df['currency_type'] = addCurrencyType(df)
    df['unit_price'] = convertEURtoUSD(df)
    df['shipping'] = fillShippingInfo(df)
    df['currency_type'] = 'USD'

    df['valid_date'] = df['timestamp'].str.extract(r'(\d{4}-\d{2}-\d{2})')
    df['valid_date'] = np.where(
        df['valid_date'].isna(),
        (df['timestamp'].str.extract(r'(\d{2}\/\d{2}\/\d{2})')[0]
                        .str.replace(r'(\d{2})\/(\d{2})\/(\d{2})', r'20\3-\1-\2', regex=True)),
        df['valid_date']
    )
    df['valid_date'] = np.where(
        df['valid_date'].isna(),
        (df['timestamp'].str.extract(r'(\d+\.\d{2}\.\d{4})')[0]
                        .str.replace(r'(\d+)\.(\d{2})\.(\d{4})', r'\3-\2-\1', regex=True)),
        df['valid_date']
    )
    df['valid_date'] = np.where(
        df['valid_date'].isna(),
        df['timestamp'].str.extract(r'(\d+-\w+\-\d+)')[0],
        df['valid_date']
    )
    def format_date(date_str):
        if type(date_str) == str:
            match = re.search(r'(\d+)-(\w+)-(\d+)', date_str)
            if match:
                day, month, year = match.groups()
                month_num = MONTHS.get(month, month)
                return f'{year}-{month_num}-{day}'
        return date_str
    
    df['data_source'] = data_source
    print(df.to_string())
    


# reprocessBooksData(data_source='DATA1')
# reprocessUsersData(data_source="DATA1")
reprocessOrdersData(data_source="DATA1")
