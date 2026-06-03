import random
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["toy"],
)
def random_outcome():
    @task
    def roll():
        if random.random() < 0.25:
            raise ValueError("Bad luck — this is the 25% failure case.")
        print("Success — rolled into the 75% window.")

    roll()


random_outcome()
