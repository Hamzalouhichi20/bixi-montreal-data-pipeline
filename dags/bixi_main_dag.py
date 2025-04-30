from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Fonction auxiliaire à l'extérieur
def categoriser_puissance(kW):
    if kW < 7:
        return "Lente"
    elif kW < 22:
        return "Normale"
    else:
        return "Rapide"

def nettoyer_et_sauvegarder():
    # Lecture du CSV brut
    df = pd.read_csv("/opt/airflow/data/bixi_montreal_trips.csv")

    # Nettoyage de base
    df.drop_duplicates(inplace=True)
    df.dropna(thresh=5, inplace=True)
    df.drop(columns=["Province", "Région", "Adresse"], errors='ignore', inplace=True)
    df = df[df["Ville"].str.strip().str.lower() == "montréal"]

    # Feature engineering
    df["Catégorie puissance"] = df["Puissance (kW)"].apply(categoriser_puissance)

    # Sauvegarde
    df.to_csv("/opt/airflow/data/bixi_cleaned.csv", index=False)

    print("Nettoyage terminé. Fichier enregistré sous bixi_cleaned.csv")
    print(df.head())

# Définition du DAG
with DAG(
    dag_id="bixi_main_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bixi", "nettoyage"]
) as dag:

    nettoyage_task = PythonOperator(
        task_id="nettoyage_et_sauvegarde_csv",
        python_callable=nettoyer_et_sauvegarder
    )