import pandas as pd 
import yaml, re, random
from load_into_db import loadIntoDB


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

def reprocessAuthor(df):
    df['author'] = (df['author']
                   .str.replace(r'\s+', r' ', regex=True)
                   .str.strip())
    return df['author']

def reprocessPublisher(df):
    most_common_publisher = df['publisher'].mode()[random.randint(0, 1)]
    df['publisher'] = df['publisher'].replace([" ", "", "NULL"], most_common_publisher)
    return df['publisher']

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
    df = df[['id', 'title', 'author', 'genre', 'publisher', 'year', 'data_source']]
    loadIntoDB(df, table_name='books')

def reprocessUsersData(data_source):
    df = pd.read_csv(f"DATAs/{data_source}/users.csv")
    df['phone'] = (df['phone']
                   .str.replace(r'[\)\.\s+]', '-', regex=True)
                   .str.replace(r'\((\d+)--', r'\1-', regex=True))
    df['address'] = df['address'].str.replace(r'')
    for i, row in df.iterrows():
        print(row['phone'])
    # print(df.head(50))

# reprocessBooksData(data_source='DATA1')
reprocessUsersData("DATA1")

