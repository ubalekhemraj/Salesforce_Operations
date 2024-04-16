from simple_salesforce import Salesforce
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import os
import schedule
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Salesforce authentication
sf = Salesforce(
    username=os.getenv('SF_USERNAME'),
    password=os.getenv('SF_PASSWORD'),
    consumer_key=os.getenv('SF_CONSUMER_KEY'),
    consumer_secret=os.getenv('SF_CONSUMER_SECRET')
)


def query_and_write_records(object_type, file_name):
    """
    Fetch records from Salesforce and write them to a CSV file.

    :param object_type: Salesforce object type to query
    :param file_name: Name of the CSV file to save records to
    """
    try:
        query = f'SELECT Id FROM {object_type} LIMIT 100'
        response = sf.query(query)
        df = pd.DataFrame([{'Id': item['Id']} for item in response['records']])
        df.to_csv(file_name, index=False)
        print(f"CSV file {file_name} saved successfully.")
    except Exception as e:
        print(f"Error occurred while querying and writing {object_type}: {e}")


def read_ids_from_csv(object_type, file_path):
    """
    Read record IDs from a CSV file and delete them from Salesforce.

    :param object_type: Salesforce object type to delete records from
    :param file_path: Path to the CSV file containing record IDs
    :return: Deletion result
    """
    df = pd.read_csv(file_path)
    records = df.to_dict(orient='records')
    result = 0
    try:
        result = getattr(sf.bulk, object_type).delete(records, batch_size=10000, use_serial=True)
        save_error_to_csv(result)
    except Exception as e:
        print(f"Error occurred while deleting {object_type} records: {e}")
    return result


def check_deleted_records(object_type, file_path):
    """
    Check if records with given IDs are deleted from Salesforce.

    :param object_type: Salesforce object type to check
    :param file_path: Path to the CSV file containing record IDs
    """
    df = pd.read_csv(file_path)
    id_list = df['Id'].tolist()
    id_str = "','".join(id_list)
    query = f"SELECT Id FROM {object_type} WHERE Id IN ('{id_str}')"
    try:
        result = sf.query_all(query)
        print(result)
    except Exception as e:
        print(f"Error occurred while checking deleted {object_type} records: {e}")


def save_error_to_csv(errors, file_path='error.csv'):
    """
    Save errors to a CSV file.

    :param errors: List of error dictionaries
    :param file_path: Path to the error CSV file
    """
    flat_errors = []
    for error in errors:
        flat_error = {
            'success': error['success'],
            'created': error['created'],
            'id': error['id'],
            'statusCode': error['errors'][0]['statusCode'] if error['errors'] else None,
            'message': error['errors'][0]['message'] if error['errors'] else None
        }
        flat_errors.append(flat_error)

    df = pd.DataFrame(flat_errors)
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            combined_data = pd.concat([existing_df, df], ignore_index=True)
            combined_data.drop_duplicates(inplace=True)
            combined_data.to_csv(file_path, index=False)
        except pd.errors.EmptyDataError:
            df.to_csv(file_path, index=False)
    else:
        df.to_csv(file_path, index=False)


def run_get_records_tasks():
    """
    Run tasks to fetch records from Salesforce.
    """
    with ThreadPoolExecutor() as executor:
        executor.submit(query_and_write_records, 'Account', 'Accounts.csv')
        executor.submit(query_and_write_records, 'Contact', 'Contacts.csv')


def run_delete_records_tasks():
    """
    Run tasks to delete records from Salesforce.
    """
    with ThreadPoolExecutor() as executor:
        executor.submit(read_ids_from_csv, 'Account', 'Accounts.csv')
        executor.submit(read_ids_from_csv, 'Contact', 'Contacts.csv')


def run_check_delete_records_tasks():
    """
    Run tasks to check deleted records from Salesforce.
    """
    with ThreadPoolExecutor() as executor:
        executor.submit(check_deleted_records, 'Account', 'Accounts.csv')
        executor.submit(check_deleted_records, 'Contact', 'Contacts.csv')


def schedule_jobs():
    """
    Schedule tasks to run at specified times.
    """
    current_time = datetime.now()
    scheduled_time = current_time + timedelta(minutes=1)
    schedule_time = scheduled_time.strftime('%H:%M')
    schedule.every().day.at(schedule_time).do(run_get_records_tasks)
    scheduled_time = current_time + timedelta(minutes=2)
    schedule_time = scheduled_time.strftime('%H:%M')
    schedule.every().day.at(schedule_time).do(run_delete_records_tasks)
    scheduled_time = current_time + timedelta(minutes=3)
    schedule_time = scheduled_time.strftime('%H:%M')
    schedule.every().day.at(schedule_time).do(run_check_delete_records_tasks)


schedule_jobs()

while True:
    schedule.run_pending()
    time.sleep(1)
