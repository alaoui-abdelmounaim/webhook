from flask import Flask, request, jsonify
import logging
import json
from togaf_csv_to_json import file_to_togaf_json
from neo4j_connector import insert_togaf_data, get_graph_data

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# --- Webhook pour importer le CSV ---
@app.route('/webhook', methods=['POST'])
def webhook():
    print("✅ Reçu POST /webhook")
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier fourni'}), 400

    file = request.files['file']
    selected_format = request.form.get("selected_format", "JSON")

    try:
        # 1) Convertir le CSV en JSON TOGAF
        togaf_json = file_to_togaf_json(file)
        print("✅ TOGAF JSON généré")

        # 2) Insérer les données dans Neo4j
        insert_togaf_data(togaf_json)
        print("✅ Données insérées dans Neo4j")

        return jsonify({
            "data": togaf_json,
            "format": selected_format
        }), 200

    except Exception as e:
        print("❌ Erreur :", str(e))
        return jsonify({"error": str(e)}), 500


@app.route('/graph', methods=['GET'])
def graph():
    try:
        graph_data = get_graph_data()

        # Transformer en texte structuré
        text_lines = []
        for e in graph_data:
            text_lines.append(
                f"{e['source']} ({','.join(e['source_labels'])}) --[{e['relation']}]--> "
                f"{e['target']} ({','.join(e['target_labels'])})"
            )

        formatted_text = "\n".join(text_lines)

        # On renvoie directement le texte brut comme body
        return formatted_text, 200, {"Content-Type": "text/plain"}

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)