import sqlite3

conn = sqlite3.connect("line_movement.db")
cursor = conn.cursor()
cursor.execute("SELECT * FROM snapshots")
rows = cursor.fetchall()
for row in rows:
    print(row)
conn.close()
