import pandas as pd

def categorize_row(row):
    domaine = str(row.get("Domaine fonctionnel", "")).lower()
    type_app = str(row.get("Type d'application", "")).lower()
    techno_dev = str(row.get("Technologie de développement (Front/Back)", "")).lower()
    hebergement = str(row.get("Hébergement", "")).lower()
    type_col = str(row.get("Type", "")).lower()
    desc = str(row.get("Description concise de l'Application", "")).lower()
    # BUSINESS ARCHITECTURE
    if "processus" in domaine or "finance" in domaine or "risques" in domaine or "distribution" in domaine or "contrôle" in domaine or "communication" in domaine:
        return "business_architecture"
    # APPLICATION ARCHITECTURE
    if "développement" in type_app or "progiciel" in type_app or "interne" in type_app or "web" in type_col or "logiciel" in type_col or "application" in type_col or "app" in type_col:
        return "application_architecture"
    # DATA ARCHITECTURE
    if "entrepôt" in type_col or "data" in type_col or "base de données" in type_col or "mariadb" in techno_dev or "oracle" in techno_dev or "sql" in techno_dev or "delphi" in techno_dev:
        return "data_architecture"
    # TECHNOLOGY ARCHITECTURE
    if "infrastructure" in hebergement or "cloud" in hebergement or "on-premise" in hebergement or "linux" in desc or "serveur" in desc or "réseau" in desc or "infrastructure" in domaine:
        return "technology_architecture"
    # fallback logique (application si aucun match, sinon uncategorized)
    if "application" in domaine or "app" in domaine:
        return "application_architecture"
    return "uncategorized"

def robust_read_csv(file):
    seps = [';', '\t', '|', ',']
    encodings = ['utf-8', 'latin1']
    for encoding in encodings:
        for sep in seps:
            try:
                file.seek(0)
                df = pd.read_csv(
                    file,
                    sep=sep,
                    engine='python',
                    on_bad_lines='skip',
                    encoding=encoding
                )
                if len(df.columns) > 2 and df.shape[0] > 0:
                    df.columns = [c.strip() for c in df.columns]
                    return df
            except Exception:
                continue
    raise RuntimeError("Impossible de lire le CSV avec séparateurs/encodages courants.")

def clean_dataframe(df):
    df = df.dropna(how="all")
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.fillna("")
    return df

def df_to_togaf_json(df):
    df = clean_dataframe(df)
    df["togaf_category"] = df.apply(categorize_row, axis=1)
    output = {
        "business_architecture": [],
        "application_architecture": [],
        "data_architecture": [],
        "technology_architecture": [],
        "uncategorized": []
    }
    for _, row in df.iterrows():
        pillar = row["togaf_category"]
        row_data = row.drop("togaf_category").to_dict()
        output[pillar].append(row_data)
    return output

def file_to_togaf_json(file):
    df = robust_read_csv(file)
    return df_to_togaf_json(df)

if __name__ == "__main__":
    import json
    with open("input.csv", "rb") as f:
        result = file_to_togaf_json(f)
        with open("output_togaf.json", "w", encoding="utf-8") as out:
            json.dump(result, out, ensure_ascii=False, indent=4)
