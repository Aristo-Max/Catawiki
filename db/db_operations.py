# serp_db.py
from db.db_connection import get_connection

def init_db():
    """Optional: create table if it doesn't exist."""
    query = """
    CREATE TABLE IF NOT EXISTS url_collection (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        category VARCHAR(255) NOT NULL,
        subcategory VARCHAR(255) NOT NULL,
        others VARCHAR(255) NOT NULL,
        url TEXT NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """
    conn = get_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
        conn.close()

def save_url(category: str, subcategory: str, others: str, url: str):
    """Save a single URL to the database (skip duplicates)."""
    conn = get_connection()
    if not conn:
        print("DB connection failed. URL not saved:", url)
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO url_collection (category, subcategory, others, url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (category, subcategory, others, url))
            conn.commit()
        return True
    except Exception as e:
        print("Error saving URL:", url, e)
        return False
    finally:
        conn.close()

def load_saved_urls(category: str, subcategory: str, others: str) -> set:
    """Return a set of URLs already saved for this subcategory & brand."""
    conn = get_connection()
    seen = set()
    if not conn:
        return seen
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT url FROM url_collection WHERE category=%s AND subcategory=%s AND others=%s
            """, (category, subcategory, others))
            rows = cur.fetchall()
            for r in rows:
                seen.add(r[0])
    except Exception as e:
        print("Error loading saved URLs:", e)
    finally:
        conn.close()
    return seen
