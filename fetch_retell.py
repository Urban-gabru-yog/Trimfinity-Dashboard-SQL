import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
from db_connection import get_connection
import time

load_dotenv()

allowed_numbers = ["+15109411358", "+12283350337", "+12082140131", "+14707630575"]

def fetch_calls_for_number(number, retell_api_key):
    url = "https://api.retellai.com/v2/list-calls"
    headers = {
        "Authorization": f"Bearer {retell_api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "limit": 1000,
        "from_number": number
    }

    for attempt in range(3):
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            break
        elif response.status_code in [429, 500, 502, 503, 504]:
            wait_time = 2 ** attempt
            print(f"⚠️ API error {response.status_code}, retrying in {wait_time}s...")
            time.sleep(wait_time)
        else:
            print(f"❌ Retell API error: {response.text}")
            return []

    try:
        resp_json = response.json()
    except Exception as e:
        print(f"❌ JSON Decode Error: {e}")
        return []

    call_list = []
    if isinstance(resp_json, list):
        call_list = resp_json
    elif isinstance(resp_json, dict):
        call_list = resp_json.get("calls", [])
    else:
        print("❌ Unexpected API response format:", resp_json)
        return []

    parsed_data = []
    for call in call_list:
        try:
            from_number = call.get("from_number", "")
            if from_number != number:
                continue
            email = call.get("retell_llm_dynamic_variables", {}).get("email")
            product_title = call.get("retell_llm_dynamic_variables", {}).get("title")
            start_ms = call.get("start_timestamp")
            end_ms = call.get("end_timestamp")
            if not start_ms or not end_ms:
                continue
            start_dt = pd.to_datetime(start_ms, unit='ms')
            end_dt = pd.to_datetime(end_ms, unit='ms')
            duration_sec = (end_dt - start_dt).total_seconds()
            total_cost = round(duration_sec * 0.00234 * 85, 4)

            raw_to_number = call.get("to_number", "")
            if raw_to_number.startswith("+91") and len(raw_to_number) > 3:
                to_number = raw_to_number[-10:]
            else:
                to_number = raw_to_number

            parsed_data.append({
                "email": email,
                "StartTimestamp": start_dt,
                "EndTimestamp": end_dt,
                "TotalDurationSec": duration_sec,
                "TotalCost": total_cost,
                "title": product_title,
                "to_number": to_number
            })
        except Exception as e:
            print(f"❌ Error parsing call: {e}")
            continue

    return parsed_data

def fetch_and_store_call_data():
    retell_api_key = os.getenv("RETELL_API_KEY")
    if not retell_api_key:
        print("❌ Missing RETELL_API_KEY in .env")
        return

    all_data = []
    for number in allowed_numbers:
        print(f"Fetching calls for {number}...")
        data = fetch_calls_for_number(number, retell_api_key)
        all_data.extend(data)

    if not all_data:
        print("⚠️ No valid call records to store.")
        return

    df = pd.DataFrame(all_data)
    print("📋 Parsed call data preview:")
    print(df.head())

    # Store in MySQL
    conn = get_connection()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO calls (email, StartTimestamp, EndTimestamp, TotalDurationSec, TotalCost, title, to_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    EndTimestamp = VALUES(EndTimestamp),
                    TotalDurationSec = VALUES(TotalDurationSec),
                    TotalCost = VALUES(TotalCost),
                    title = VALUES(title),
                    to_number = VALUES(to_number)
            """, (
                row["email"], row["StartTimestamp"], row["EndTimestamp"],
                row["TotalDurationSec"], row["TotalCost"], row["title"], row["to_number"]
            ))
        except Exception as e:
            print(f"❌ DB insert failed: {e}")
            continue
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Call data with TotalCallingCost stored in MySQL")

if __name__ == "__main__":
    fetch_and_store_call_data()
