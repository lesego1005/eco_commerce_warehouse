import psycopg2
from datetime import datetime, timedelta
import holidays  # pip install holidays (for ZA holidays)

# Connect to DB
conn = psycopg2.connect(dbname='eco_warehouse', user='postgres', password='Money12!', host='localhost')
cursor = conn.cursor()

# Clear existing if needed
cursor.execute("DELETE FROM dim_date;")

# ZA Holidays
za_holidays = holidays.ZA()

start_date = datetime(2020, 1, 1)
end_date = datetime(2026, 12, 31)
current_date = start_date

while current_date <= end_date:
    date_str = current_date.date()
    year = current_date.year
    quarter = (current_date.month - 1) // 3 + 1
    month = current_date.month
    day = current_date.day
    weekday = current_date.strftime('%A')
    holiday_flag = date_str in za_holidays
    holiday_name = za_holidays.get(date_str) if holiday_flag else None
    
    cursor.execute("""
        INSERT INTO dim_date (date, year, quarter, month, day, weekday, holiday_flag, holiday_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """, (date_str, year, quarter, month, day, weekday, holiday_flag, holiday_name))
    
    current_date += timedelta(days=1)

conn.commit()
conn.close()
print("dim_date populated!")