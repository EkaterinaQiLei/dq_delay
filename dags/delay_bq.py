import logging
from datetime import datetime, timedelta
from itertools import chain
from typing import Any, Dict, List

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models import Pool, Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.utils.weight_rule import WeightRule

DEFAULT_ARGS = {
    "depends_on_past": False,
    "owner": "airflow",
    "pool": Pool.DEFAULT_POOL_NAME,
    "weight_rule": WeightRule.ABSOLUTE,
    "priority_weight": 10,  # todo: your priority
}

with DAG(
    dag_id="delay_bq",
    description="Check last available date in gbq tables, if it's different from current_date, loads the difference",
    start_date=datetime(2024, 1, 15),
    schedule=timedelta(hours=4),
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["bigquery", "monitoring"],
) as dag:
    dag_vars = Variable.get("delay_bq_config", deserialize_json=True)

    @task
    def fetch_customers() -> List[Dict[str, Any]]:
        # todo: your filters
        example_customers = [
            {"id": 1, "name": "Customer A", "modules": ["example2", "example1"]},
            {"id": 2, "name": "Customer B", "modules": ["example5"]},
        ]
        return list(
            batched(
                filter(lambda c: len(c["modules"]) > 0, example_customers),
                batch_size=dag_vars.get("batch_size", 10),
            )
        )

    @task
    def check_module_delays(customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not customers:
            return []

        bq_client = BigQueryHook(gcp_conn_id="google_cloud_default").get_client()
        delay_dataframes = []

        for customer in customers:
            # todo: your logic
            # example of a dataframe
            df = pd.DataFrame([
                {"customer_id": customer["customer_id"], "customer_name": customer["customer_name"],
                 "module": module, "lag": 1}
                for module in customer["modules"]
            ])
            delay_dataframes.append(df)

        delays = pd.concat(delay_dataframes, ignore_index=True)
        return delays.to_dict(orient="records")

    @task
    def upload_to_sheets(delays: List[List[Dict[str, Any]]]):
        if not delays or not any(delays):
            logging.info("No delays")
            return

        gsheets_hook = GSheetsHook(gcp_conn_id="google_sheets_default")  # todo: conn_id
        spreadsheet_id = dag_vars["spreadsheet_id"]

        flat_delays = list(chain(*delays))
        df = pd.DataFrame(flat_delays)

        pivot_df = (
            df.pivot_table(
                index=["id", "name"],
                columns="needed",
                values="lag",
                aggfunc="first",
            )
            .fillna(0)
            .reset_index()
        )

        # todo: list of columns
        expected_modules = ["example1", "example2", "example3", "example4"]
        for mod in expected_modules:
            if mod not in pivot_df.columns:
                pivot_df[mod] = 0

        final_df = pivot_df[["value", "value"] + expected_modules]

        gsheets_hook.clear(spreadsheet_id=spreadsheet_id, range_="A2:Z1000")
        gsheets_hook.update_values(
            spreadsheet_id=spreadsheet_id,
            range_="A2",
            values=final_df.values.tolist(),
        )

    check_delays = check_module_delays.expand(customers=fetch_customers())
    upload_to_sheets(delays=check_delays)
