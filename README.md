# Delay Monitoring

Airflow job which

check delays in bigquery
group by value
loads data to google sheet

## Install

```bash
pip install -r requirements.txt
```
## Settings

1. Airflow:  
   - **ID**: `delay_bq`
   - **type**: JSON
   - **value**:

```json
{
  "batch_size": 10,
  "delay_days_threshold": 2,
  "spreadsheet_id": "your-google-sheet-id"
}
```

2. Airflow connection:

| Conn ID               | Type           | Comm                 |
|-----------------------|----------------|--------------------------------|
| `google_cloud_default` | Google Cloud   | BigQuery         |
| `google_sheets_default` | Google Cloud  | Sheets    |

## Local launch

```bash
cp .env.example .env
export $(cat .env | xargs)
airflow dags trigger delay_bq
```

## Structure

```
bigquery-delay-monitoring/
├── dags/
│   └── delay_bq.py
├── secrets/
│   └── service-account.json
├── .env.example
├── requirements.txt
├── README.md
└── .gitignore
