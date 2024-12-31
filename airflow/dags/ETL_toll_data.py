from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import tarfile
import csv


default_args = {
    'owner': 'Vimalesh',
    'start_date': datetime.now(),  
    'email': ['vim@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily'
)

def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    dest_dir = "/home/project/airflow/dags/python_etl/staging"
    os.makedirs(dest_dir, exist_ok=True)
    file_path = os.path.join(dest_dir, "tolldata.tgz")
    response = requests.get(url)
    with open(file_path, 'wb') as file:
        file.write(response.content)

def untar_dataset():
    source_path = "/home/project/airflow/dags/python_etl/staging/tolldata.tgz"
    dest_dir = "/home/project/airflow/dags/python_etl/staging"
    with tarfile.open(source_path) as tar:
        tar.extractall(path=dest_dir)

def extract_data_from_csv():
    source_file = "/home/project/airflow/dags/python_etl/staging/vehicle-data.csv"
    dest_file = "/home/project/airflow/dags/python_etl/staging/csv_data.csv"
    with open(source_file, 'r') as infile, open(dest_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(["Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type"])
        for row in reader:
            writer.writerow(row[:4])

def extract_data_from_tsv():
    source_file = "/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv"
    dest_file = "/home/project/airflow/dags/python_etl/staging/tsv_data.csv"
    with open(source_file, 'r') as infile, open(dest_file, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile)
        writer.writerow(["Number of axles", "Tollplaza id", "Tollplaza code"])
        for row in reader:
            writer.writerow([row[4], row[5], row[6]])

def extract_data_from_fixed_width():
    source_file = "/home/project/airflow/dags/python_etl/staging/payment-data.txt"
    dest_file = "/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv"
    with open(source_file, 'r') as infile, open(dest_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Type of Payment code", "Vehicle Code"])
        for line in infile:
            writer.writerow([
                line[50:70].strip(),  
                line[70:90].strip()  
            ])

def consolidate_data():
    csv_file = "/home/project/airflow/dags/python_etl/staging/csv_data.csv"
    tsv_file = "/home/project/airflow/dags/python_etl/staging/tsv_data.csv"
    fixed_width_file = "/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv"
    consolidated_file = "/home/project/airflow/dags/python_etl/staging/extracted_data.csv"
    
    with open(csv_file, 'r') as csv_in, open(tsv_file, 'r') as tsv_in, open(fixed_width_file, 'r') as fixed_width_in, open(consolidated_file, 'w', newline='') as consolidated_out:
        csv_reader = csv.DictReader(csv_in)
        tsv_reader = csv.DictReader(tsv_in)
        fixed_width_reader = csv.DictReader(fixed_width_in)

        writer = csv.DictWriter(consolidated_out, fieldnames=[
            "Rowid", "Timestamp", "Anonymized Vehicle number", "Vehicle type",
            "Number of axles", "Tollplaza id", "Tollplaza code", "Type of Payment code", "Vehicle Code"
        ])
        writer.writeheader()

        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_width_reader):
            writer.writerow({
                "Rowid": csv_row["Rowid"],
                "Timestamp": csv_row["Timestamp"],
                "Anonymized Vehicle number": csv_row["Anonymized Vehicle number"],
                "Vehicle type": csv_row["Vehicle type"],
                "Number of axles": tsv_row["Number of axles"],
                "Tollplaza id": tsv_row["Tollplaza id"],
                "Tollplaza code": tsv_row["Tollplaza code"],
                "Type of Payment code": fixed_row["Type of Payment code"],
                "Vehicle Code": fixed_row["Vehicle Code"]
            })

def transform_data():
    source_file = "/home/project/airflow/dags/python_etl/staging/extracted_data.csv"
    dest_file = "/home/project/airflow/dags/python_etl/staging/transformed_data.csv"
    with open(source_file, 'r') as infile, open(dest_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row["Vehicle type"] = row["Vehicle type"].upper()
            writer.writerow(row)


download_dataset_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

untar_dataset_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_data_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

download_dataset_task >> untar_dataset_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_data_task >> transform_data_task
