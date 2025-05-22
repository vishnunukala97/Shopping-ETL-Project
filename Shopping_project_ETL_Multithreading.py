import pyodbc
import uuid
import random
import json
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor

# Load JSON file
try:
    with open('Items.json') as f:
        items = json.load(f)
except FileNotFoundError:
    print("We did not find any JSON file")
    exit()
except json.JSONDecodeError:
    print("Invalid JSON format")
    exit()

if not items:
    print("Empty JSON")
    exit()

# Validate JSON records
required_fields = ['item_id', 'item_name', 'price', 'is_active', 'number_records']
seen_items = set()
valid_items = []
total_items = len(items)
valid_item_count = 0
skipped_item_count = 0

for item in items:
    if not all(field in item for field in required_fields):
        skipped_item_count += 1
        continue
    if item['item_id'] in seen_items:
        skipped_item_count += 1
        continue
    if not (isinstance(item['price'], (int, float)) and isinstance(item['is_active'], bool)):
        skipped_item_count += 1
        continue
    if not (0.01 <= item['price'] <= 1000):
        skipped_item_count += 1
        continue
    if not item['item_id'].startswith("ITM"):
        skipped_item_count += 1
        continue
    if not (3 <= len(item['item_name']) <= 200):
        skipped_item_count += 1
        continue
    if not (isinstance(item['number_records'], int) and item['number_records'] > 0):
        skipped_item_count += 1
        continue

    seen_items.add(item['item_id'])
    valid_items.append(item)
    valid_item_count += 1

if not valid_items:
    print("No valid items found")
    exit()

# Chunk function
def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

item_chunks = list(chunk_list(valid_items, 1000))

# Worker thread function
def process_chunk(items_chunk):
    import pyodbc
    import uuid
    import random
    from datetime import datetime, timedelta

    server = 'Vishnu\\SQLEXPRESS'
    database = 'ShoppingDB'
    try:
        conn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'Trusted_Connection=yes;'
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"[Thread] DB connection failed: {e}")
        return

    timezones = ['PST', 'EST', 'CST', 'MST']
    insert_item_count = 0
    insert_txn_count = 0

    for item in items_chunk:
        item_id = item['item_id']
        cursor.execute("SELECT COUNT(*) FROM Items WHERE item_id = ?", item_id)
        exists = cursor.fetchone()[0]

        if exists == 0:
            try:
                cursor.execute(
                    "INSERT INTO Items (item_id, item_name, price, is_active) VALUES (?, ?, ?, ?)",
                    item_id, item['item_name'], item['price'], int(item['is_active'])
                )
                insert_item_count += 1
            except Exception as e:
                print(f"[Thread] Failed to insert item {item_id}: {e}")
                continue

        for _ in range(item['number_records']):
            try:
                cursor.execute(
                    "INSERT INTO Transactions (transaction_id, item_id, date_of_transaction, quantity, timezone) VALUES (?, ?, ?, ?, ?)",
                    str(uuid.uuid4()), item_id,
                    (datetime.now() - timedelta(days=random.randint(0, 3))).date(),
                    random.randint(1, 10),
                    random.choice(timezones)
                )
                insert_txn_count += 1
            except Exception as e:
                print(f"[Thread] Failed to insert transaction for {item_id}: {e}")
                continue

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[Thread]  Inserted {insert_item_count} items, {insert_txn_count} transactions")

# Run threads
start_time = time.time()

with ThreadPoolExecutor(max_workers=3) as executor:
    executor.map(process_chunk, item_chunks)

end_time = time.time()

# Summary
print("\n ETL Summary")
print("------------------------------")
print(f"Total items in JSON:         {total_items}")
print(f"Valid items processed:       {valid_item_count}")
print(f"Skipped due to validation:   {skipped_item_count}")
print(f"Time taken:                   {end_time - start_time:.2f} seconds")
