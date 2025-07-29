from neo4j import GraphDatabase

# Identifiants AuraDB
URI = "neo4j+s://b921ab91.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "Vj4e0_o2Gpn_0tJ-XvJSURdVOsY196UhGjQn_xYPxJA"
DATABASE = "neo4j"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


def insert_togaf_data(togaf_data):
    """
    Insère les nœuds TOGAF dans le graphe Neo4j.
    """
    with driver.session(database=DATABASE) as session:
        for category, entries in togaf_data.items():
            for entry in entries:
                name = entry.get("Nom de l'application", "Unknown")
                session.run(
                    """
                    MERGE (n:Component {name: $name})
                    SET n.category = $category
                    """,
                    name=name,
                    category=category
                )


def get_graph_data():
    """
    Récupère le graphe entier en filtrant les orphelins et en ajoutant les labels.
    """
    query = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN coalesce(n.nom, n.name) AS source,
           labels(n) AS source_labels,
           type(r) AS relation,
           coalesce(m.nom, m.name) AS target,
           labels(m) AS target_labels
    """
    with driver.session(database=DATABASE) as session:
        result = session.run(query)
        data = []
        for record in result:
            # Exclure les noeuds complètement isolés
            if record["relation"] is None and record["target"] is None:
                continue
            data.append({
                "source": record["source"],
                "source_labels": record["source_labels"],
                "relation": record["relation"],
                "target": record["target"],
                "target_labels": record["target_labels"]
            })
    return data
