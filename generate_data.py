# generate_data.py
import pandas as pd
import json
import os
import random
from datetime import datetime, timedelta
import argparse
import numpy as np

# -------------------------------
# Configurable settings
# -------------------------------
PRODUCTS = [
    {"name": "Portable Solar Charger 20W", "category": "Solar", "price": 899.00, "carbon_rating": 2},
    {"name": "Bamboo Toothbrush Set (4-pack)", "category": "Personal Care", "price": 120.00, "carbon_rating": 1},
    {"name": "Reusable Beeswax Food Wraps", "category": "Kitchen", "price": 250.00, "carbon_rating": 1},
    {"name": "Eco-Friendly Laundry Detergent", "category": "Cleaning", "price": 180.00, "carbon_rating": 3},
    {"name": "Stainless Steel Straw Set", "category": "Kitchen", "price": 89.00, "carbon_rating": 1},
    {"name": "Recycled Cotton Tote Bag", "category": "Accessories", "price": 150.00, "carbon_rating": 2},
    {"name": "Solar Garden Lights (6-pack)", "category": "Outdoor", "price": 450.00, "carbon_rating": 3},
]

# Some South African cities/regions for realism
CITIES = ["Johannesburg", "Cape Town", "Durban", "Pretoria", "Bloemfontein", "Gqeberha", "East London", None]  # None = missing

LOYALTY_LEVELS = ["Bronze", "Silver", "Gold", "Green Hero"]

def parse_args():
    parser = argparse.ArgumentParser(description="Generate sample Eco-Commerce data")
    parser.add_argument("--date", type=str, default=None, help="Date in YYYY-MM-DD (default: today)")
    return parser.parse_args()

def main():
    args = parse_args()
    if args.date:
        try:
            sim_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print("Invalid date format. Using today.")
            sim_date = datetime.now()
    else:
        sim_date = datetime.now()

    today_str = sim_date.strftime("%Y-%m-%d")
    folder = f"raw_data/{today_str}"
    os.makedirs(folder, exist_ok=True)

    print(f"Generating data for {today_str} → {folder}")

    # 1. Sales CSV - 80-150 rows, realistic variation + issues
    n_sales = random.randint(80, 150)
    sales_list = []
    used_sale_ids = set()

    for _ in range(n_sales):
        product = random.choice(PRODUCTS)
        qty = random.choices([1, 2, 3, 5, 10], weights=[0.5, 0.2, 0.15, 0.1, 0.05])[0]

        # Introduce outliers ~2-3%
        if random.random() < 0.025:
            qty = random.randint(500, 2000)  # Crazy high quantity

        price_str = f"R{product['price']:.2f}" if random.random() < 0.15 else str(product['price'])  # Sometimes string with 'R'

        sale_id = random.randint(100000, 999999)
        while sale_id in used_sale_ids:  # ~5% duplicate chance
            sale_id = random.randint(100000, 999999)
        used_sale_ids.add(sale_id)

        sales_list.append({
            "sale_id": sale_id,
            "date": today_str,
            "sale_timestamp": (sim_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))).isoformat(),
            "product_name": product["name"],  # FIXED: always use exact original name (no random lowercasing)
            "quantity": qty,
            "price": price_str,
            "customer_email": f"customer_{random.randint(100,999)}@example.co.za",
            "city": random.choice(CITIES)
        })

    # Add ~8% duplicate rows
    if len(sales_list) > 10:
        dupes = random.sample(sales_list, k=max(3, len(sales_list)//12))
        sales_list.extend(dupes)

    sales_df = pd.DataFrame(sales_list)
    sales_df = sales_df.sample(frac=1).reset_index(drop=True)  # Shuffle
    sales_path = f"{folder}/sales_{today_str}.csv"
    sales_df.to_csv(sales_path, index=False)
    print(f"Created {sales_path} ({len(sales_df)} rows)")

    # 2. Products JSON - full catalog (small updates possible in future runs)
    products_path = f"{folder}/products_{today_str}.json"
    with open(products_path, "w", encoding="utf-8") as f:
        json.dump(PRODUCTS, f, indent=2)
    print(f"Created {products_path} ({len(PRODUCTS)} products)")

    # 3. Customers Excel - growing list, some new, some duplicates/missing
    # Simulate ~5-15 new customers per "day"
    n_customers = random.randint(5, 15)
    customers_list = []

    first_names = ["Lesego", "Thabo", "Amahle", "Sipho", "Nomsa", "Lungelo", "Zanele", "Kagiso", "Refilwe", "Mpho"]
    domains = ["gmail.com", "outlook.com", "yahoo.co.za", "example.co.za", "mweb.co.za"]

    for i in range(n_customers):
        name = f"{random.choice(first_names)} {random.choice(['Mokoena', 'Nkosi', 'Dlamini', 'Naidoo', 'van der Merwe'])}"
        email = f"{name.lower().replace(' ', '.')}@{random.choice(domains)}"
        join_date = (sim_date - timedelta(days=random.randint(10, 730))).strftime("%Y-%m-%d")  # Joined in last ~2 years

        customers_list.append({
            "customer_name": name,
            "email": email,
            "loyalty_level": random.choice(LOYALTY_LEVELS),
            "join_date": join_date
        })

    # Add one missing email occasionally
    if random.random() < 0.2 and customers_list:
        customers_list[0]["email"] = None

    customers_df = pd.DataFrame(customers_list)
    customers_path = f"{folder}/customers_{today_str}.xlsx"
    customers_df.to_excel(customers_path, index=False, sheet_name="Customers")
    print(f"Created {customers_path} ({len(customers_df)} rows)")

    print("\nDone! Ready to ingest these files in Phase 3.")

if __name__ == "__main__":
    main()