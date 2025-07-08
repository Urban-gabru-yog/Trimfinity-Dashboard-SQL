import pandas as pd
from db_connection import get_connection

def merge_data():
    conn = get_connection()

    # Load data from DB
    df_calls = pd.read_sql("SELECT * FROM calls", conn)
    df_orders = pd.read_sql("SELECT * FROM shopify_orders", conn)

    # Normalize column names
    df_calls.columns = df_calls.columns.str.strip()
    df_orders.columns = df_orders.columns.str.strip()

    # Basic cleanup
    df_orders["created_at"] = pd.to_datetime(df_orders["created_at"], errors="coerce")
    df_orders["order_date"] = df_orders["created_at"].dt.date
    df_orders["price"] = pd.to_numeric(df_orders["total_price"], errors="coerce").fillna(0)

    # Clean and normalize phone numbers for joining
    df_calls["to_number"] = df_calls["to_number"].astype(str).str.replace(r"\D", "", regex=True).str[-10:]
    df_orders["phone"] = df_orders["phone"].astype(str).str.replace(r"\D", "", regex=True).str[-10:]

    # Merge on phone = to_number
    df_merged = pd.merge(
        df_orders,
        df_calls,
        how="inner",
        left_on="phone",
        right_on="to_number"
    )

    # Prioritize title from orders table, fallback to calls table
    df_merged["title"] = df_merged["title_x"].fillna(df_merged["title_y"])
    df_merged["title_clean"] = df_merged["title"].astype(str).str.strip().str.lower()

    # Merge with COGS
    try:
        cogs_df = pd.read_sql("SELECT * FROM product_cogs", conn)
        cogs_df["product_title_clean"] = cogs_df["product_title"].astype(str).str.strip().str.lower()
        df_merged = df_merged.merge(
            cogs_df[["product_title_clean", "cogs"]],
            left_on="title_clean",
            right_on="product_title_clean",
            how="left"
        )
        df_merged["COGS"] = pd.to_numeric(df_merged["cogs"], errors="coerce").fillna(0)
    except Exception as e:
        print(f"⚠️ DB COGS merge error: {e}")
        df_merged["COGS"] = 0

    # Deduplicate to keep only one unique record per product per order per phone
    df_merged = df_merged.sort_values(by=["phone", "order_number", "created_at"], ascending=[True, True, False])
    df_merged = df_merged.drop_duplicates(subset=["phone", "order_number", "title"], keep="first")

    # Insert into merged_data table
    # Insert into merged_data table
    cursor = conn.cursor()
    for _, row in df_merged.iterrows():
        try:
            # email = row.get("email")
            email = row.get("email_x") if pd.notna(row.get("email_x")) and str(row.get("email_x")).strip() else row.get("email_y")
            if pd.isna(email) or not str(email).strip():
                email = "NA"

            values = tuple(None if pd.isna(x) else x for x in [
                email, row.get("StartTimestamp"),
                row.get("TotalDurationSec"), row.get("TotalCost"),
                row.get("order_number"), row.get("created_at"), row.get("total_price"),
                row.get("discount_codes"), row.get("customer_first_name"),
                row.get("title"), row.get("COGS")
            ])
            cursor.execute("""
                INSERT INTO merged_data (
                    Email, StartTimestamp, TotalDurationSec, TotalCost,
                    order_number, created_at, total_price, discount_codes,
                    customer_first_name, title, COGS
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Email = VALUES(Email),
                    StartTimestamp = VALUES(StartTimestamp),
                    TotalDurationSec = VALUES(TotalDurationSec),
                    TotalCost = VALUES(TotalCost),
                    created_at = VALUES(created_at),
                    total_price = VALUES(total_price),
                    discount_codes = VALUES(discount_codes),
                    customer_first_name = VALUES(customer_first_name),
                    COGS = VALUES(COGS)
            """, values)

        except Exception as e:
            print(f"⚠️ Skipping row due to error: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Merged data (joined on phone ↔ to_number) stored in MySQL")

if __name__ == "__main__":
    merge_data()
