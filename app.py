from flask import Flask, request, jsonify
import logging
import json
from neo4j_togaf_connector import process_csv_to_neo4j, get_graph_data

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Endpoint pour importer un fichier CSV et le transformer automatiquement 
    en graphe TOGAF dans Neo4j
    """
    logger.info("✅ Reçu POST /webhook")
    
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier fourni'}), 400
    
    file = request.files['file']
    selected_format = request.form.get("selected_format", "TOGAF_NEO4J")
    
    if file.filename == '':
        return jsonify({'error': 'Nom de fichier vide'}), 400
    
    if not file.filename.lower().endswith('.csv'):
        return jsonify({'error': 'Le fichier doit être un CSV'}), 400
    
    try:
        # Traitement automatique CSV -> TOGAF -> Neo4j
        result = process_csv_to_neo4j(file)
        logger.info("✅ Données transformées et insérées dans Neo4j avec succès")
        
        return jsonify({
            "status": "success",
            "message": result["message"],
            "format": selected_format,
            "entities_created": result["entities_created"],
            "details": {
                "applications": result["entities_created"].get("physical_applications", 0),
                "capabilities": result["entities_created"].get("business_capabilities", 0),
                "services": result["entities_created"].get("business_services", 0),
                "functions": result["entities_created"].get("business_functions", 0),
                "data_entities": result["entities_created"].get("data_entities", 0),
                "technology_components": result["entities_created"].get("technology_components", 0),
                "vendors": result["entities_created"].get("vendors", 0)
            }
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du traitement : {str(e)}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "message": "Erreur lors de la transformation des données"
        }), 500

@app.route('/graph', methods=['GET'])
def graph():
    """
    Endpoint pour récupérer le graphe TOGAF depuis Neo4j
    """
    try:
        graph_data = get_graph_data()
        logger.info(f"✅ Récupéré {len(graph_data)} relations du graphe")
        
        # Transformer en texte structuré pour l'affichage
        text_lines = []
        
        # Grouper par type de relation pour une meilleure lisibilité
        grouped_relations = {}
        for e in graph_data:
            relation_type = e['relation'] if e['relation'] else 'NO_RELATION'
            if relation_type not in grouped_relations:
                grouped_relations[relation_type] = []
            grouped_relations[relation_type].append(e)
        
        # Formatter par groupe
        for relation_type, relations in grouped_relations.items():
            if relation_type != 'NO_RELATION':
                text_lines.append(f"\n=== {relation_type} ===")
                for e in relations:
                    source_labels = ', '.join(e['source_labels']) if e['source_labels'] else 'Unknown'
                    target_labels = ', '.join(e['target_labels']) if e['target_labels'] else 'Unknown'
                    text_lines.append(
                        f"{e['source']} ({source_labels}) --[{e['relation']}]--> "
                        f"{e['target']} ({target_labels})"
                    )
        
        formatted_text = "\n".join(text_lines)
        
        return formatted_text, 200, {"Content-Type": "text/plain; charset=utf-8"}
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération du graphe : {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/graph/json', methods=['GET'])
def graph_json():
    """
    Endpoint pour récupérer le graphe TOGAF en format JSON
    """
    try:
        graph_data = get_graph_data()
        logger.info(f"✅ Récupéré {len(graph_data)} relations du graphe en JSON")
        
        return jsonify({
            "status": "success",
            "data": graph_data,
            "count": len(graph_data)
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération du graphe JSON : {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/stats', methods=['GET'])
def stats():
    """
    Endpoint pour obtenir des statistiques sur le graphe TOGAF
    """
    try:
        from neo4j_togaf_connector import GraphDatabase, URI, USER, PASSWORD, DATABASE
        
        driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
        
        with driver.session(database=DATABASE) as session:
            # Compter les différents types de nœuds
            stats_query = """
            MATCH (n)
            RETURN labels(n)[0] as label, count(n) as count
            ORDER BY count DESC
            """
            result = session.run(stats_query)
            
            node_stats = {}
            total_nodes = 0
            for record in result:
                label = record["label"]
                count = record["count"]
                node_stats[label] = count
                total_nodes += count
            
            # Compter les relations
            rel_query = """
            MATCH ()-[r]->()
            RETURN type(r) as relation, count(r) as count
            ORDER BY count DESC
            """
            result = session.run(rel_query)
            
            relation_stats = {}
            total_relations = 0
            for record in result:
                relation = record["relation"]
                count = record["count"]
                relation_stats[relation] = count
                total_relations += count
        
        driver.close()
        
        return jsonify({
            "status": "success",
            "statistics": {
                "total_nodes": total_nodes,
                "total_relations": total_relations,
                "node_types": node_stats,
                "relation_types": relation_stats
            }
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du calcul des statistiques : {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """
    Endpoint de santé pour vérifier la connectivité
    """
    try:
        from neo4j_togaf_connector import GraphDatabase, URI, USER, PASSWORD, DATABASE
        
        driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
        with driver.session(database=DATABASE) as session:
            result = session.run("RETURN 1 as test")
            test_result = result.single()["test"]
        
        driver.close()
        
        if test_result == 1:
            return jsonify({
                "status": "healthy",
                "neo4j_connection": "OK",
                "message": "Tous les services sont opérationnels"
            }), 200
        else:
            return jsonify({
                "status": "unhealthy",
                "neo4j_connection": "FAILED"
            }
