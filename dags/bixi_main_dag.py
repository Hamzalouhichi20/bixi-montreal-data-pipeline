from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# ======== Cleaning AND Feature Engineering ========
def process_and_save():
    df = pd.read_csv("/opt/airflow/data/bixi_montreal_trips.csv")
    df.drop_duplicates(inplace=True)
    df.dropna(thresh=5, inplace=True)
    df["Ville"] = df["Ville"].str.strip().str.upper()
    df["is_fast_charge"] = df["Niveau de recharge"].str.contains("BRCC", na=False)
    df["price_mode_code"] = df["Mode de tarification"].map({
        "par heure": 0,
        "par session": 1,
        "par palier (à partir de)": 2
    })
    df["is_on_street"] = df["Type d’emplacement"].str.contains("sur rue", case=False, na=False)
    df["high_power"] = df["Puissance (kW)"] >= 50
    df["lat_zone"] = pd.cut(df["Latitude"], bins=10, labels=False)
    df["lon_zone"] = pd.cut(df["Longitude"], bins=10, labels=False)
    df.drop(columns=[
        "Ville", "Mode de tarification", "Niveau de recharge", "Province",
        "Région", "Adresse", "Suite", "Type d’emplacement", "Puissance (kW)"
    ], errors="ignore", inplace=True)
    df.to_csv("/opt/airflow/data/bixi_final.csv", index=False)
    print(df.head())

# ======== Run external scripts using exec() ========
def run_eda():
    namespace = {}
    with open("/opt/airflow/scripts/EDA.py") as f:
        exec(f.read(), namespace)
    namespace["analyse_donnees"]()

def run_train_model():
    namespace = {}
    with open("/opt/airflow/scripts/train_model.py") as f:
        exec(f.read(), namespace)
    namespace["train_model"]()

def run_predict_model():
    namespace = {}
    with open("/opt/airflow/scripts/predict_model.py") as f:
        exec(f.read(), namespace)
    namespace["predict_tarif_mode"]()

# ======== Définition du DAG ========
with DAG(
    dag_id="bixi_main_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["bixi", "pipeline"]
) as dag:

    nettoyage_task = PythonOperator(
        task_id="clean_and_engineer_features",
        python_callable=process_and_save
    )

    analyse_rues_task = PythonOperator(
        task_id="analyse_donnees",
        python_callable=run_eda
    )

    train_task = PythonOperator(
        task_id="train_price_mode_model",
        python_callable=run_train_model
    )

    predict_task = PythonOperator(
        task_id="predict_price_mode",
        python_callable=run_predict_model
    )

    # Dépendances
    nettoyage_task >> analyse_rues_task >> train_task >> predict_task