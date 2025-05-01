import pandas as pd
def analyse_donnees():
    df = pd.read_csv("/opt/airflow/data/bixi_final.csv")
    print("Shape:", df.shape)
    print("Description:\n", df.describe())
    print("Value counts:\n", df["is_fast_charge"].value_counts())
     # Comptage des bornes par rue et par niveau de recharge
    result = df.groupby(["Rue", "Niveau de recharge"]).size().reset_index(name="nombre_de_bornes")
    print("Résumé des bornes par rue et niveau de recharge :")
    print(result.head(10))