from airflow.decorators import task, dag, task_group
from datetime import datetime


@dag(start_date=datetime(2023, 1, 1))
def process_many_files():

    @task
    def process_file(filename: str):
        print(f"Processing file {filename}")

    @task
    def get_files():
        files = [f"file_{idx}.txt" for idx in range(10)]
        return files


    process_file.expand(filename=get_files())


process_many_files()