import pandas as pd
import joblib

def predict_tarif_mode():    
    df = pd.read_csv("/opt/airflow/data/bixi_final.csv")
    model = joblib.load("/opt/airflow/data/bixi_tarif_model.pkl")
    df_result = df.copy()
    X = df.drop(columns=[
        "price_mode_code",  # parfois présente
        "Nom de la borne de recharge",
        "Nom du parc",
        "Rue",
        "Code Postal"
    ], errors="ignore")
    X = pd.get_dummies(X)
    preds = model.predict(X)
    df_result["predicted_tarif_code"] = preds
    df_result.to_csv("/opt/airflow/data/bixi_predictions.csv", index=False)
    print("✅ Predictions saved to 'bixi_predictions.csv'")
    print(df_result[["predicted_tarif_code"]].head())