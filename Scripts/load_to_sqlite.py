import os, sqlite3, pandas as pd

# Find base directory (project root)
BASE = os.path.dirname(os.path.dirname(__file__))

# Path where raw CSVs are stored
RAW = os.path.join(BASE, "data", "raw")

# SQLite database file
DB_PATH = os.path.join(BASE, "ecommerce.sqlite")
print("Looking for CSVs in:", RAW)
print("Files found:", os.listdir(RAW))

def main():
    conn = sqlite3.connect(DB_PATH)  # create/open SQLite database
    try:
        for f in os.listdir(RAW):  # loop through all files in data/raw
            if f.endswith(".csv"):
                path = os.path.join(RAW, f)
                table = os.path.splitext(f)[0].lower()  # use filename as table name
                print(f"Loading {f} -> {table}")
                df = pd.read_csv(path, encoding="utf-8", low_memory=False)
                df.columns = [c.strip().lower() for c in df.columns]  # clean column names
                df.to_sql(table, conn, if_exists="replace", index=False)
        print("✅ Done. Database created at", DB_PATH)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
