import json
import random
import string

total_records = 150
garbage_ratio = 0.2
garbage_count = int(total_records * garbage_ratio)
valid_count = total_records - garbage_count

items = []

# ✅ Helper to generate random strings
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

# ✅ Generate valid items
for i in range(valid_count):
    item = {
        "item_id": f"ITM{i:04}",
        "item_name": f"Product_{i}",
        "price": round(random.uniform(10, 500), 2),
        "is_active": random.choice([True, False]),
        "number_records": random.randint(1, 30)
    }
    items.append(item)

# ❌ Add 20% garbage items
for i in range(garbage_count):
    choice = random.choice(["missing_field", "negative_price", "bad_type", "bad_id", "bad_name"])

    if choice == "missing_field":
        item = {"item_id": f"BAD{i:04}"}  # Missing most fields
    elif choice == "negative_price":
        item = {
            "item_id": f"ITM_NEG{i:04}",
            "item_name": f"Negative_{i}",
            "price": -random.uniform(1, 50),
            "is_active": True,
            "number_records": random.randint(1, 10)
        }
    elif choice == "bad_type":
        item = {
            "item_id": f"ITM_BADTYPE{i:04}",
            "item_name": f"TypeFail_{i}",
            "price": "thirty",  # Invalid type
            "is_active": "yes",  # Invalid type
            "number_records": "ten"
        }
    elif choice == "bad_id":
        item = {
            "item_id": f"X{i:04}",  # Doesn't start with "ITM"
            "item_name": f"BadID_{i}",
            "price": round(random.uniform(5, 100), 2),
            "is_active": True,
            "number_records": random.randint(1, 5)
        }
    elif choice == "bad_name":
        item = {
            "item_id": f"ITM_BADNAME{i:04}",
            "item_name": random_string(2),  # Too short
            "price": round(random.uniform(10, 100), 2),
            "is_active": True,
            "number_records": random.randint(1, 5)
        }

    items.append(item)

# ✅ Save to items.json
with open("items.json", "w") as f:
    json.dump(items, f, indent=4)

print(f"✅ Generated {total_records} items (valid: {valid_count}, garbage: {garbage_count})")
