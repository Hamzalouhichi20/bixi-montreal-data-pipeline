import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib

def train_model():
    df = pd.read_csv("/opt/airflow/data/bixi_final.csv")
    df = df.dropna(subset=["price_mode_code"])
    y = df["price_mode_code"]
    X = df.drop(columns=[
        "price_mode_code",  # cible
        "Nom de la borne de recharge",
        "Nom du parc",
        "Rue",
        "Code Postal"
    ], errors="ignore")
    X = pd.get_dummies(X)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"Model accuracy: {acc:.2f}")
    print(classification_report(y_test, y_pred))
    joblib.dump(model, "/opt/airflow/data/bixi_tarif_model.pkl")
    print("Model saved to 'bixi_tarif_model.pkl'")