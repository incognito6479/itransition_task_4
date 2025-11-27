import pandas as pd 


def reprocessBooks():
    df = pd.read_parquet("DATAs/DATA1/orders.parquet")
    print(df.head(50), '\n')
    print(len(df), 'rows')