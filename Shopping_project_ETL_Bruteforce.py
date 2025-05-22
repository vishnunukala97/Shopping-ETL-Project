import pyodbc
import uuid
from datetime import datetime,timedelta
import random
import json
import time

# SQL Server connection details
server = 'Vishnu\\SQLEXPRESS'
database = 'ShoppingDB'

# Establish connection
conn = pyodbc.connect(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'Trusted_Connection=yes;'
)

cursor = conn.cursor()

# Load JSON file
try:
    with open('Items.json') as f:
        items = json.load(f)
except FileNotFoundError:
    print(" We did not find any JSON file.")
    exit()
except json.JSONDecodeError:
    print(" Invalid JSON format.")
    exit()

if not items:
    print(" Empty JSON.")
    exit()

# Validation the data in the JSON file
required_fields = ['item_id', 'item_name', 'price', 'is_active','number_records']
seen_items = set()
valid_items = []
total_items = len(items)
valid_items_count = 0
skipped_items_count = 0
total_transactions_inserted = 0


for item in items:
    if not all(field in item for field in required_fields):
        print(f" Missing fields in item: {item}, skip")
        skipped_items_count += 1
        continue

    if item['item_id'] in seen_items:
        print(f" Duplicate Item found in JSON: {item['item_id']}, skip")
        skipped_items_count += 1
        continue

    if not (isinstance(item['price'], (int, float)) and isinstance(item['is_active'], bool)):
        print(f"Invalid datatype for item: {item}, skip")
        skipped_items_count += 1
        continue

    if not(0.01 <= item['price'] <= 1000):
        print(f"Price out of range for item: {item['item_id']} -> {item['price']}, skip")
        skipped_items_count += 1
        continue

    if not item['item_id'].startswith("ITM"):
        print(f"Inavlid item_id format:{item['item_id']},skip")
        skipped_items_count += 1
        continue

    if not (3 <= len(item['item_name']) <= 200 ):
        print(f"Inavlid item_name length:{item['item_name']},skip")
        skipped_items_count += 1
        continue

    if not isinstance(item['number_records'], int) or item['number_records'] <= 0:
        print(f"Invalid 'transactions' count for item: {item['item_id']}, skip")
        skipped_items_count += 1
        continue
    




    seen_items.add(item['item_id'])
    valid_items.append(item)
    valid_items_count += 1


if not valid_items:
    print("No valid items found.")
    exit()

start_time = time.time() 

# Define possible timezones
timezones = ['PST', 'EST', 'CST', 'MST']

# Process valid items
for item in valid_items:
    item_id = item['item_id']
    item_name = item['item_name']
    price = item['price']
    is_active = item['is_active']

    # Check if item already exists
    cursor.execute("SELECT COUNT(*) FROM Items WHERE item_id = ?", item_id)
    exists = cursor.fetchone()[0]

    # Insert into Items table if not exists
    if exists == 0:
        try:
            cursor.execute(
                "INSERT INTO Items (item_id, item_name, price, is_active) VALUES (?, ?, ?, ?)",
                item_id, item_name, price, int(is_active)
            )
            print(f" Inserted new item: {item_id}")
        except Exception as e:
            print(f" Failed to insert item {item_id}: {e}")
            continue

    # Insert into Transactions table
    for _ in range(item['number_records']):
        try:
            transaction_id = str(uuid.uuid4())
            days_ago = random.randint(0, 3)
            date_of_transaction = (datetime.now() - timedelta(days=days_ago)).date()
            quantity = random.randint(1, 10)
            timezone = random.choice(timezones)

            cursor.execute(
            "INSERT INTO Transactions (transaction_id, item_id, date_of_transaction, quantity, timezone) VALUES (?, ?, ?, ?, ?)",
            transaction_id, item_id, date_of_transaction, quantity, timezone
            )
            total_transactions_inserted += 1

       
            print(f"Inserted transaction for item: {item_id}")
        except Exception as e:
            print(f"Failed to insert transaction for {item_id}: {e}")
            continue

conn.commit()
cursor.close()
conn.close()

end_time = time.time()

print("\nETL Summary Report")
print("----------------------------")
print(f"Total items in JSON:         {total_items}")
print(f"Valid items processed:       {valid_items_count}")
print(f"Items skipped due to errors: {skipped_items_count}")
print(f"Transactions inserted:       {total_transactions_inserted}")
print(f"Time taken:                   {end_time - start_time:.2f} seconds")
