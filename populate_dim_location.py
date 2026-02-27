# populate_dim_location.py
import psycopg2

conn = psycopg2.connect(dbname='eco_warehouse', user='postgres', password='Money12!', host='localhost')
cursor = conn.cursor()

# Clear if needed
cursor.execute("DELETE FROM dim_location;")

locations = [
    ('Johannesburg', 'South Africa', 'Gauteng'),
    ('Cape Town', 'South Africa', 'Western Cape'),
    ('Durban', 'South Africa', 'KwaZulu-Natal'),
    ('Pretoria', 'South Africa', 'Gauteng'),
    ('Bloemfontein', 'South Africa', 'Free State'),
    ('Gqeberha', 'South Africa', 'Eastern Cape'),
    ('East London', 'South Africa', 'Eastern Cape'),
]

for city, country, region in locations:
    cursor.execute("""
        INSERT INTO dim_location (city, country, region)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (city, country, region))

conn.commit()
cursor.close()
conn.close()
print("dim_location populated with 7 SA cities!")