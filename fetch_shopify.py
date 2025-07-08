import requests
import json
import os
from dotenv import load_dotenv
from db_connection import get_connection

load_dotenv()

def fetch_and_store_shopify_orders():
    store = os.getenv("SHOPIFY_STORE")
    token = os.getenv("SHOPIFY_ACCESS_TOKEN")

    print("DEBUG: SHOPIFY_STORE =", os.getenv("SHOPIFY_STORE"))
    print("DEBUG: SHOPIFY_ACCESS_TOKEN =", "SET" if os.getenv("SHOPIFY_ACCESS_TOKEN") else "NOT SET")

    url = f"https://{store}/admin/api/2023-01/orders.json"
    params = {
        "status": "any",
        "limit": 250  # Fetches the latest 250 orders
    }
    headers = {"X-Shopify-Access-Token": token}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"❌ Shopify API error: {response.status_code}")
        return

    orders = response.json().get("orders", [])
    print(f"✅ Total orders fetched: {len(orders)}")

    # Insert into DB
    conn = get_connection()
    cursor = conn.cursor()

    for order in orders:
        try:
            email = order.get("email") or None
            order_number = order.get("order_number") or None
            created_at = order.get("created_at") or None
            total_price = order.get("total_price") or "0"
            discount_codes = json.dumps(order.get("discount_codes") or [])
            customer_first_name = (
                order.get("customer", {}).get("first_name") if order.get("customer") else None
            )

            line_items = order.get("line_items")
            title = None
            if isinstance(line_items, list) and len(line_items) > 0:
                title = line_items[0].get("title")

            line_items_json = json.dumps(line_items)

            # ✅ Robust phone extraction
            phone = None
            if order.get("billing_address") and order["billing_address"].get("phone"):
                phone = order["billing_address"]["phone"]
            elif order.get("phone"):
                phone = order["phone"]

            cursor.execute("""
                INSERT INTO shopify_orders (
                    email, order_number, created_at, total_price,
                    discount_codes, customer_first_name, line_items, title, phone
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    email = VALUES(email),
                    created_at = VALUES(created_at),
                    total_price = VALUES(total_price),
                    discount_codes = VALUES(discount_codes),
                    customer_first_name = VALUES(customer_first_name),
                    line_items = VALUES(line_items),
                    title = VALUES(title),
                    phone = VALUES(phone)
            """, (
                email, order_number, created_at, total_price,
                discount_codes, customer_first_name, line_items_json, title, phone
            ))

        except Exception as e:
            print(f"❌ Skipping order {order.get('order_number')} due to error: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Shopify data stored in MySQL")

if __name__ == "__main__":
    fetch_and_store_shopify_orders()
