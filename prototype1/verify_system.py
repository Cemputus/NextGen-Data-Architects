"""Quick system verification script"""
from sqlalchemy import create_engine, text
from config import DATA_WAREHOUSE_CONN_STRING, DB1_CONN_STRING, DB2_CONN_STRING

def verify_database(conn_string, db_name):
    """Verify database connection and data"""
    try:
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            # Check if we can connect
            result = conn.execute(text("SELECT DATABASE()"))
            current_db = result.fetchone()[0]
            print(f"[OK] Connected to {db_name}: {current_db}")
            return True
    except Exception as e:
        print(f"[ERROR] Error connecting to {db_name}: {e}")
        return False

def check_tables(conn_string, db_name):
    """Check if tables exist and have data"""
    try:
        engine = create_engine(conn_string)
        with engine.connect() as conn:
            if db_name == 'UCU_DataWarehouse':
                result = conn.execute(text("SELECT COUNT(*) FROM dim_student"))
                count = result.fetchone()[0]
                print(f"  -> dim_student table has {count} rows")
            elif db_name == 'UCU_SourceDB1':
                result = conn.execute(text("SELECT COUNT(*) FROM students"))
                count = result.fetchone()[0]
                print(f"  -> students table has {count} rows")
            return True
    except Exception as e:
        print(f"  [ERROR] Error checking tables: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("UCU System Verification")
    print("=" * 60)
    
    print("\n[1] Checking Source Database 1 (UCU_SourceDB1)...")
    verify_database(DB1_CONN_STRING, "UCU_SourceDB1")
    check_tables(DB1_CONN_STRING, "UCU_SourceDB1")
    
    print("\n[2] Checking Source Database 2 (UCU_SourceDB2)...")
    verify_database(DB2_CONN_STRING, "UCU_SourceDB2")
    check_tables(DB2_CONN_STRING, "UCU_SourceDB2")
    
    print("\n[3] Checking Data Warehouse (UCU_DataWarehouse)...")
    verify_database(DATA_WAREHOUSE_CONN_STRING, "UCU_DataWarehouse")
    check_tables(DATA_WAREHOUSE_CONN_STRING, "UCU_DataWarehouse")
    
    print("\n" + "=" * 60)
    print("Verification Complete!")
    print("=" * 60)

