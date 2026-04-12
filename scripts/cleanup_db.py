import sqlite3
import sys
from pathlib import Path

def main():
    root_dir = Path(__file__).resolve().parents[1]
    db_path = root_dir / "data" / "app.db"
    
    if not db_path.exists():
        print(f"Database not found at {db_path}. Skipping cleanup.")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Delete llm_delta events
        cursor.execute("DELETE FROM analysis_events WHERE event_type = 'llm_delta'")
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"Deleted {deleted_count} 'llm_delta' events from database.")
        
        # Vacuum the database to reclaim space
        cursor.execute("VACUUM")
        conn.commit()
        
        print("Database VACUUM completed.")
        conn.close()
    except Exception as e:
        print(f"Error during database cleanup: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
