import os, sqlite3, pandas as pd

BASE = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE, "ecommerce.sqlite")
OUT_DIR = os.path.join(BASE, "data", "processed")
os.makedirs(OUT_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

queries = {
    "monthly_revenue": """
        SELECT substr(o.order_purchase_timestamp,1,7) as month,
               SUM(p.payment_value) as revenue
        FROM olist_orders_dataset o
        JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
        GROUP BY month ORDER BY month;
    """,
    "top_customers": """
        SELECT o.customer_id, SUM(p.payment_value) as total_spent
        FROM olist_orders_dataset o
        JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
        GROUP BY o.customer_id ORDER BY total_spent DESC LIMIT 10;
    """,
    "top_categories": """
        SELECT pr.product_category_name, COUNT(*) as total_orders
        FROM olist_order_items_dataset oi
        JOIN olist_products_dataset pr ON oi.product_id = pr.product_id
        JOIN olist_orders_dataset o ON oi.order_id = o.order_id
        GROUP BY pr.product_category_name
        ORDER BY total_orders DESC LIMIT 10;
    """
}

for name, sql in queries.items():
    df = pd.read_sql_query(sql, conn)
    df.to_csv(os.path.join(OUT_DIR, f"{name}.csv"), index=False)
    print(f"✅ Exported {name}.csv")

conn.close()
