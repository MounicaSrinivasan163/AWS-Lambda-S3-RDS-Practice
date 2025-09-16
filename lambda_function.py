import csv
import json
import boto3
import pymysql

# S3 client
s3 = boto3.client('s3')

# RDS connection details (⚠️ use Secrets Manager in prod)
rds_host = "<your-RDS-endpoint>"
rds_user = "admin"
rds_password = "<your-password>"
rds_port = 3306
db_name = "<yourdb>"   # fixed database name

def lambda_handler(event, context):
    print("✅ Lambda triggered by S3 upload")
    
    # Get bucket and file name
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    # Table name = file name without extension
    table_name = file_key.split('.')[0]
    
    print(f"📂 File: {file_key}, Database: {db_name}, Table: {table_name}")
    
    # Read CSV from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.reader(body)
    
    # Extract header row (column names)
    header = next(reader)
    print(f"📝 Header: {header}")
    
    # Connect to RDS
    try:
        conn = pymysql.connect(host=rds_host,
                               user=rds_user,
                               passwd=rds_password,
                               port=rds_port,
                               connect_timeout=5)
        cursor = conn.cursor()
        print("✅ Connected to RDS")
    except Exception as e:
        print("❌ Could not connect to RDS:", e)
        return {"statusCode": 500, "body": str(e)}
    
    try:
        # Ensure database exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;")
        cursor.execute(f"USE `{db_name}`;")
        
        # Drop table if it already exists (optional)
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        
        # Create table using CSV header
        col_defs = ", ".join([f"`{col}` VARCHAR(255)" for col in header])
        create_sql = f"CREATE TABLE `{table_name}` ({col_defs});"
        cursor.execute(create_sql)
        print(f"✅ Table `{table_name}` created with columns: {header}")
        
        # Build INSERT query dynamically
        placeholders = ", ".join(["%s"] * len(header))
        insert_sql = f"INSERT INTO `{table_name}` ({','.join(header)}) VALUES ({placeholders})"
        
        # Insert all rows
        for row in reader:
            cursor.execute(insert_sql, row)
        
        conn.commit()
        print("✅ Data inserted successfully")
    except Exception as e:
        conn.rollback()
        print("❌ Insert failed:", e)
        return {"statusCode": 500, "body": str(e)}
    finally:
        cursor.close()
        conn.close()
        print("🔒 Connection closed")
    
    return {
        "statusCode": 200,
        "body": json.dumps(f"CSV data inserted into DB: {db_name}, Table: {table_name}")
    }
