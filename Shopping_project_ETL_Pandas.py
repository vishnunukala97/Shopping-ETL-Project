import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import uuid
import random
import time

engine = create_engine("mssql+pyodbc://Vishnu\\SQLEXPRESS/ShoppingDB?driver=ODBC+Driver+17+for+SQL+Server")
conn = engine.connect()


existing_items_df = pd.read_sql("select item_id from Items",conn)
existing_ids = set(existing_items_df['item_id'].tolist())

df = pd.read_json("Items.json")

total_records = len(df)

required_feilds = ['item_id','item_name','price','is_active','number_records']
df = df.dropna(subset = required_feilds)

df = df[
    df['item_id'].str.startswith("ITM")&
    df['item_name'].str.len().between(3,200)&
    df['price'].apply(lambda x: isinstance(x,(int,float)) and x >0)&
    df['is_active'].apply(lambda x: isinstance(x,bool))&
    df['number_records'].apply(lambda x: isinstance(x,int) and x >0 )
]

valid_items = len(df)

df_new_items = df[~df['item_id'].isin(existing_ids)].copy()
df_existing_items = df[df['item_id'].isin(existing_ids)].copy()

if not df_new_items.empty:
    df_new_items[['item_id','item_name','price','is_active']].to_sql(
        'Items', con =engine, if_exists='append', index= False
    )

final_items_df = pd.concat([df_existing_items,df_new_items],ignore_index= True)

txn_rows = []
timezones = ['PST','EST','CST','MST']

start_time = time.time() 

for _,row in final_items_df.iterrows():
    for _ in range(row['number_records']):
        txn_rows.append({
            'transaction_id': str(uuid.uuid4()),
            'item_id' : row['item_id'],
            'date_of_transaction' : (datetime.now() - timedelta(days = random.randint(0,3))).date(),
            'quantity':random.randint(1,30),
            'timezone' : random.choice(timezones)
            })

txn_df = pd.DataFrame(txn_rows)

txn_df.to_sql('Transactions',con=engine, if_exists='append',index=False,chunksize=1000)

end_time = time.time()

print("New items inserted:", len(df_new_items))
print("Transactions inserted:", len(txn_df))
print(f"Time taken:{end_time - start_time:.2f} seconds")
