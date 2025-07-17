import sqlite3
import requests
import csv
import io
import os

# section 1: Create the database (exam_data.db)
# I did this in the command line terminal, but I could have also done:

# connect to database
conn = sqlite3.connect('/Users/sarahboyer/exam_Data.db')
cursor = conn.cursor()

cursor.execute("PRAGMA foreign_keys = ON")

# Create exams table
cursor.execute("""
CREATE TABLE IF NOT EXISTS exams_Table (
    id INTEGER PRIMARY KEY,
    year INTEGER,
    language_pair TEXT
)
""")

# Create errors table
cursor.execute("""
CREATE TABLE IF NOT EXISTS errors_Table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exam_id INTEGER,
    error_type TEXT,
    error_severity TEXT,
    FOREIGN KEY (exam_id) REFERENCES exams_Table(id)
)
""")

# Save
conn.commit()

# database check
print("Writing to:", os.path.abspath('exam_Data.db'))

# section 2: Read data from exams.csv

url = 'https://pastebin.com/raw/e4WamPB4'
response = requests.get(url)

# error testing
if response.status_code != 200:
    print("Error fetching CSV from Pastebin.")
    exit()


# Section 3: Insert the data into the appropriate tables
csv_data = list(csv.DictReader(io.StringIO(response.text)))

# Insert into exams_Table
for row in csv_data:
    cursor.execute("""INSERT OR IGNORE INTO exams_Table (id, year, language_pair)
                      VALUES (?, ?, ?)
                   """, (
                       row['id'],
                       row['year'],
                       row['language_pair']
                   ))

# Insert into errors_Table
for row in csv_data:
    cursor.execute("""INSERT OR IGNORE INTO errors_Table (id, exam_id, error_type, error_severity)
                      VALUES (?, ?, ?, ?)
                   """, (
                       row['id'],
                       row['id'],  # using same ID as exam_id, assuming 1:1 match
                       row['error_type'],
                       row['error_severity']
                   ))

conn.commit()

print("Data successfully saved to SQLite!")

# section 4: run the 3 queries

# exams count query

query1 = """
SELECT COUNT(DISTINCT exams_table.id)
FROM exams_Table
LEFT JOIN errors_Table ON errors_Table.exam_id = exams_Table.id
GROUP BY exams_Table.language_pair
    """

cursor.execute(query1)

count1 = cursor.fetchone()[0]

print(f"Total number of exams per language pair: {count1}")

# common error types
query2 = """
    SELECT error_type, COUNT(*) AS error_count
    FROM errors_Table
    GROUP BY error_type
    order by error_count desc
    LIMIT 5; """

cursor.execute(query2)

top_errors = cursor.fetchall()

print("Top 5 most common error types:")
for error_type, count in top_errors:
    print(f"{error_type}: {count}")


#spelling error query
query3 = """
SELECT COUNT(*) 
FROM errors_Table
JOIN exams_Table ON errors_Table.exam_id = exams_Table.id
WHERE language_pair = ?
  AND error_type = ?
  AND year = ?
"""

params = ("English-French", "Spelling", 2017)

cursor.execute(query3, params)

count3 = cursor.fetchone()[0]

print(f"Spelling errors in English-French exams from 2017: {count3}")


conn.close()

