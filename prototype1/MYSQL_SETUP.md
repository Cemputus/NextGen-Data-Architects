# MySQL Setup Guide

This project uses MySQL instead of SQLite or SQL Server. Follow these steps to set up MySQL.

## Prerequisites

1. **Install MySQL**
   - Download MySQL Community Server from https://dev.mysql.com/downloads/mysql/
   - Install MySQL with default settings
   - During installation, set a root password
   - Note: For Windows, you can also use MySQL Installer

2. **Verify Installation**
   ```bash
   mysql --version
   ```

3. **Start MySQL Service**
   - **Windows**: MySQL service should start automatically, or use Services app
   - **Linux**: `sudo systemctl start mysql`
   - **Mac**: `brew services start mysql` or use System Preferences

## Configuration

Update `config.py` with your MySQL credentials:

```python
MYSQL_HOST = 'localhost'  # or your MySQL server address
MYSQL_PORT = '3306'       # default MySQL port
MYSQL_USER = 'root'       # or your MySQL username
MYSQL_PASSWORD = ''       # Your MySQL password
MYSQL_CHARSET = 'utf8mb4' # Character set
```

Or set environment variables:

```bash
# Windows
set MYSQL_HOST=localhost
set MYSQL_PORT=3306
set MYSQL_USER=root
set MYSQL_PASSWORD=YourPassword123!

# Linux/Mac
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=YourPassword123!
```

## Verify Connection

Test your connection:

```python
import pymysql
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD

try:
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")
```

## Create Databases

### Option 1: Using SQL Scripts

Execute the SQL scripts in the `sql/` directory:

```bash
mysql -u root -p < sql/create_source_db1.sql
mysql -u root -p < sql/create_source_db2.sql
mysql -u root -p < sql/create_data_warehouse.sql
mysql -u root -p < sql/populate_time_dimension.sql
```

### Option 2: Using Python Scripts

The Python scripts will automatically create databases:

```bash
python setup_databases.py
python etl_pipeline.py
```

## Troubleshooting

### "Access denied" error
- Verify username and password
- Check if user has CREATE DATABASE privileges
- Try: `mysql -u root -p` to test login

### "Can't connect to MySQL server" error
- Verify MySQL service is running
- Check firewall settings (port 3306)
- Verify host and port in config.py
- Try: `mysqladmin ping` to test server

### "Unknown database" error
- Databases are created automatically by setup scripts
- Ensure you have CREATE DATABASE privileges
- Manually create: `CREATE DATABASE UCU_SourceDB1;`

### "Table already exists" error
- This is normal if running setup multiple times
- Tables use `IF NOT EXISTS` so they won't be recreated
- To reset: `DROP DATABASE UCU_SourceDB1;` then rerun setup

### Character encoding issues
- Ensure charset is `utf8mb4` in config.py
- Verify MySQL server charset: `SHOW VARIABLES LIKE 'character_set%';`
- All tables use `utf8mb4_unicode_ci` collation

### Time dimension population fails
- Requires MySQL 8.0+ for recursive CTE
- For MySQL 5.7, use Python script to populate (handled automatically)
- Check: `SELECT VERSION();` to see MySQL version

## Database Names

The system creates three databases:
- `UCU_SourceDB1` - First source database
- `UCU_SourceDB2` - Second source database  
- `UCU_DataWarehouse` - Data warehouse

## Alternative: Using MySQL Workbench

1. Open MySQL Workbench
2. Connect to your MySQL server
3. Open each SQL file from `sql/` directory
4. Execute each script (Ctrl+Shift+Enter)

## Security Best Practices

Instead of using root user, create a dedicated user:

```sql
-- In MySQL
CREATE USER 'university_user'@'localhost' IDENTIFIED BY 'SecurePassword123!';
GRANT ALL PRIVILEGES ON UCU_SourceDB1.* TO 'university_user'@'localhost';
GRANT ALL PRIVILEGES ON UCU_SourceDB2.* TO 'university_user'@'localhost';
GRANT ALL PRIVILEGES ON UCU_DataWarehouse.* TO 'university_user'@'localhost';
FLUSH PRIVILEGES;
```

Then update config.py:
```python
MYSQL_USER = 'university_user'
MYSQL_PASSWORD = 'SecurePassword123!'
```

## Performance Tuning

For large datasets, consider:

```sql
-- Increase buffer pool size (in my.cnf or my.ini)
innodb_buffer_pool_size = 1G

-- Enable query cache
query_cache_size = 64M
query_cache_type = 1

-- Increase max connections
max_connections = 200
```

## Backup and Restore

```bash
# Backup
mysqldump -u root -p UCU_SourceDB1 > backup_db1.sql
mysqldump -u root -p UCU_SourceDB2 > backup_db2.sql
mysqldump -u root -p UCU_DataWarehouse > backup_warehouse.sql

# Restore
mysql -u root -p UCU_SourceDB1 < backup_db1.sql
```




