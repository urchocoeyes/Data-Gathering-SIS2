from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from scraper import scrape_matches
from cleaner import clean_matches
from loader import create_database, load_data

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'football_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for football match data using Selenium',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    tags=['football', 'selenium', 'data_pipeline']
)

def task_scrape(**context):
    print("Starting Selenium web scraper...")
    df = scrape_matches()
    print(f"Scraped {len(df)} matches")
    
    context['ti'].xcom_push(key='scraped_count', value=len(df))
    return len(df)

def task_clean(**context):
    print("Cleaning scraped data...")
    
    ti = context['ti']
    scraped_count = ti.xcom_pull(task_ids='scrape_data', key='scraped_count')
    print(f"Received {scraped_count} matches from scraping task")
    
    df = clean_matches()
    print(f"Cleaned {len(df)} matches")
    
    context['ti'].xcom_push(key='cleaned_count', value=len(df))
    return len(df)

def task_load(**context):
    print("Loading data to database...")
    
    # Get count from cleaning task
    ti = context['ti']
    cleaned_count = ti.xcom_pull(task_ids='clean_data', key='cleaned_count')
    print(f"Received {cleaned_count} matches from cleaning task")
    
    create_database()
    count = load_data()
    
    if count >= 100:
        print(f"SUCCESS: {count} matches loaded to database")
        print(f"Database: data/football.db")
        return count
    else:
        error_msg = f"Only {count} matches loaded (minimum 100 required)"
        print(f"FAILED: {error_msg}")
        raise ValueError(error_msg)

# Create tasks
t1 = PythonOperator(
    task_id='scrape_data',
    python_callable=task_scrape,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='clean_data',
    python_callable=task_clean,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=task_load,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
t1 >> t2 >> t3

# For manual execution (without Airflow)
def run_pipeline_manually():
    """Run the complete pipeline manually"""
    print("=" * 60)
    print("RUNNING FOOTBALL DATA PIPELINE (MANUAL EXECUTION)")
    print("=" * 60)
    
    try:
        # Step 1: Scrape
        print("\n[1/3] Scraping data with Selenium...")
        df_scraped = scrape_matches()
        print(f"Scraped {len(df_scraped)} matches")
        
        # Step 2: Clean
        print("\n[2/3] Cleaning data...")
        df_clean = clean_matches()
        print(f"Cleaned {len(df_clean)} matches")
        
        # Step 3: Load
        print("\n[3/3] Loading to database...")
        create_database()
        count = load_data()
        
        if count >= 100:
            print("\n" + "=" * 60)
            print("PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"Total matches in database: {count}")
            print(f"Database file: data/football.db")
            print(f"\nVerify with:")
            print(f" sqlite3 data/football.db \\\"SELECT COUNT(*) FROM matches;\\\"")
            return True
        else:
            print(f"\nPIPELINE FAILED: Only {count} matches")
            return False
            
    except Exception as e:
        print(f"\n ERROR: {e}")
        return False

if __name__ == "__main__":
    # Run pipeline when script is executed directly
    success = run_pipeline_manually()
    exit(0 if success else 1)