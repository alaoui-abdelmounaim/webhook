from neo4j import GraphDatabase
import pandas as pd
import json
from typing import Dict, List, Any
import logging

# Configuration Neo4j
URI = "neo4j+s://b921ab91.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "Vj4e0_o2Gpn_0tJ-XvJSURdVOsY196UhGjQn_xYPxJA"
DATABASE = "neo4j"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TogafNeo4jMapper:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
        
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Nettoie la base de données"""
        with self.driver.session(database=DATABASE) as session:
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared")
    
    def create_indexes(self):
        """Crée les index pour optimiser les performances"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (app:PhysicalApplicationComponent) ON (app.nom)",
            "CREATE INDEX IF NOT EXISTS FOR (app:PhysicalApplicationComponent) ON (app.acronyme)",
            "CREATE INDEX IF NOT EXISTS FOR (app:PhysicalApplicationComponent) ON (app.criticite)",
            "CREATE INDEX IF NOT EXISTS FOR (bc:BusinessCapability) ON (bc.nom)",
            "CREATE INDEX IF NOT EXISTS FOR (bs:BusinessService) ON (bs.nom)",
            "CREATE INDEX IF NOT EXISTS FOR (de:DataEntity) ON (de.nom)",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (app:PhysicalApplicationComponent) REQUIRE app.id IS UNIQUE"
        ]
        
        with self.driver.session(database=DATABASE) as session:
            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    logger.warning(f"Index creation failed: {e}")
    
    def map_csv_to_togaf_entities(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Mappe les données CSV vers les entités TOGAF"""
        
        entities = {
            'physical_applications': [],
            'business_capabilities': [],
            'business_services': [],
            'business_functions': [],
            'data_entities': [],
            'logical_data_components': [],
            'physical_data_components': [],
            'technology_components': [],
            'vendors': []
        }
        
        for _, row in df.iterrows():
            # Extraction des données principales
            app_data = {
                'id': str(len(entities['physical_applications']) + 1),
                'nom': row.get('Nom de l\'application', ''),
                'acronyme': row.get('Acronyme', ''),
                'type_domaine': row.get('Domaine fonctionnel', ''),
                'description': row.get('Description concise de l\'Application', ''),
                'type_application': row.get('Type d\'application', ''),
                'criticite': row.get('Criticité', 'P3 - Normal'),
                'statut': row.get('Statut', 'Actif'),
                'utilisateurs': row.get('Nb utilisateurs', 'Moins de 100'),
                'frequence_utilisation': row.get('Fréquence d\'utilisation', 'Quotidien'),
                'frequence_releases': row.get('Fréquence des releases', 'Trimestriel'),
                'hebergement': row.get('Hébergement', 'On-premise'),
                'strategie_cloud': self._determine_cloud_strategy(row),
                'technologies': self._extract_technologies(row),
                'cout_annuel': row.get('Coût annuel', '0'),
                'maintenance_annuelle': row.get('Maintenance annuelle', '0'),
                'commentaires': row.get('Commentaires', '')
            }
            
            entities['physical_applications'].append(app_data)
            
            # Création des Business Capabilities basées sur le domaine
            capability = self._create_business_capability(row)
            if capability and capability not in entities['business_capabilities']:
                entities['business_capabilities'].append(capability)
            
            # Création des Business Services
            service = self._create_business_service(row)
            if service and service not in entities['business_services']:
                entities['business_services'].append(service)
            
            # Création des Business Functions
            function = self._create_business_function(row)
            if function and function not in entities['business_functions']:
                entities['business_functions'].append(function)
            
            # Création des Data Entities
            data_entity = self._create_data_entity(row)
            if data_entity:
                entities['data_entities'].append(data_entity)
            
            # Création des composants techniques
            tech_component = self._create_technology_component(row)
            if tech_component:
                entities['technology_components'].append(tech_component)
            
            # Création des vendors
            vendor = self._create_vendor(row)
            if vendor and vendor not in entities['vendors']:
                entities['vendors'].append(vendor)
        
        return entities
    
    def _determine_cloud_strategy(self, row) -> str:
        """Détermine la stratégie cloud basée sur les caractéristiques de l'application"""
        hebergement = str(row.get('Hébergement', '')).lower()
        type_app = str(row.get('Type d\'application', '')).lower()
        technologies = str(row.get('Technologie de développement (Front/Back)', '')).lower()
        
        if 'cloud' in hebergement:
            return 'Cloud Native'
        elif 'développement interne' in type_app and any(tech in technologies for tech in ['java', 'spring', 'react']):
            return 'Refactor'
        elif 'progiciel' in type_app:
            return 'Retain ou Rehost'
        else:
            return 'TBD'
    
    def _extract_technologies(self, row) -> List[str]:
        """Extrait les technologies utilisées"""
        tech_field = str(row.get('Technologie de développement (Front/Back)', ''))
        if not tech_field or tech_field == 'nan':
            return []
        
        # Sépare les technologies par des délimiteurs communs
        technologies = []
        for delimiter in [',', ';', '/', '|', '+']:
            if delimiter in tech_field:
                technologies = [t.strip() for t in tech_field.split(delimiter)]
                break
        
        return technologies if technologies else [tech_field.strip()]
    
    def _create_business_capability(self, row) -> Dict:
        """Crée une Business Capability basée sur le domaine fonctionnel"""
        domaine = row.get('Domaine fonctionnel', '')
        if not domaine:
            return None
            
        capability_mapping = {
            'Finance, contrôle de gestion et comptabilité': 'Comptabilité Temps Réel',
            'Risques': 'Analytics Risques',
            'Distribution & communication': 'Banque Digitale Omnicanale',
            'Paiements & transaction banking': 'Paiements Temps Réel',
            'Capacités techniques': 'Gestion Identités Centralisée',
            'Processus d\'entreprise': 'Traitement Documentaire Intelligent',
            'Cockpit': 'Pilotage et Reporting'
        }
        
        capability_name = capability_mapping.get(domaine, f"Capability {domaine}")
        
        return {
            'nom': capability_name,
            'niveau_maturite': 'Intermédiaire',
            'domaine': domaine,
            'criticite_metier': self._map_criticality_to_business(row.get('Criticité', '')),
            'niveau_automatisation': '60%'
        }
    
    def _create_business_service(self, row) -> Dict:
        """Crée un Business Service"""
        nom_app = row.get('Nom de l\'application', '')
        if not nom_app:
            return None
            
        return {
            'nom': f"Service {nom_app}",
            'type': 'Core',
            'domaine': row.get('Domaine fonctionnel', ''),
            'sla': self._determine_sla(row.get('Criticité', '')),
            'cout_annuel': row.get('Coût annuel', '0')
        }
    
    def _create_business_function(self, row) -> Dict:
        """Crée une Business Function"""
        domaine = row.get('Domaine fonctionnel', '')
        if not domaine:
            return None
            
        return {
            'nom': domaine,
            'niveau': 'L1',
            'budget_annuel': '2M€',
            'effectif': '50 personnes'
        }
    
    def _create_data_entity(self, row) -> Dict:
        """Crée une Data Entity basée sur l'application"""
        nom_app = row.get('Nom de l\'application', '')
        domaine = row.get('Domaine fonctionnel', '')
        
        if not nom_app:
            return None
        
        # Détermine le type de données basé sur le domaine
        data_type_mapping = {
            'Finance': 'Écriture Comptable',
            'Risques': 'Risque',
            'Paiements': 'Paiement',
            'Distribution': 'Client',
            'Processus': 'Document'
        }
        
        data_type = 'Transaction'  # Par défaut
        for key, value in data_type_mapping.items():
            if key.lower() in domaine.lower():
                data_type = value
                break
        
        return {
            'nom': data_type,
            'type': 'Master' if data_type in ['Client', 'Document'] else 'Transactional',
            'sensibilite': 'High',
            'volume_quotidien': '10000',
            'retention': '7 ans',
            'classification': 'Secret'
        }
    
    def _create_technology_component(self, row) -> Dict:
        """Crée un Technology Component"""
        technologies = self._extract_technologies(row)
        if not technologies:
            return None
        
        # Prend la première technologie comme composant principal
        main_tech = technologies[0]
        
        return {
            'nom': main_tech,
            'version': '1.0',
            'type': 'Runtime',
            'provider': 'Unknown',
            'licence': 'Commercial',
            'support_fin': '2030'
        }
    
    def _create_vendor(self, row) -> Dict:
        """Crée un Vendor basé sur le type d'application"""
        type_app = row.get('Type d\'application', '')
        
        if 'développement interne' in type_app.lower():
            vendor_name = 'EAI'
        elif 'progiciel' in type_app.lower():
            # Essaie d'extraire le nom du vendor du nom de l'application
            nom_app = row.get('Nom de l\'application', '')
            vendor_name = nom_app.split()[0] if nom_app else 'Unknown Vendor'
        else:
            vendor_name = 'Unknown Vendor'
        
        return {
            'nom': vendor_name,
            'type': 'Software',
            'criticite': 'Strategic' if row.get('Criticité', '') == 'P1 - Très critique' else 'Tactical',
            'pays': 'Unknown',
            'contrat_fin': '2025-12-31',
            'satisfaction': '8/10'
        }
    
    def _map_criticality_to_business(self, criticite: str) -> str:
        """Mappe la criticité technique vers la criticité business"""
        mapping = {
            'P1 - Très critique': 'Critical',
            'P2 - Critique': 'High',
            'P3 - Normal': 'Medium'
        }
        return mapping.get(criticite, 'Medium')
    
    def _determine_sla(self, criticite: str) -> str:
        """Détermine le SLA basé sur la criticité"""
        mapping = {
            'P1 - Très critique': '99.9%',
            'P2 - Critique': '99.5%',
            'P3 - Normal': '99.0%'
        }
        return mapping.get(criticite, '99.0%')
    
    def insert_togaf_entities(self, entities: Dict[str, List[Dict]]):
        """Insère toutes les entités TOGAF dans Neo4j"""
        
        with self.driver.session(database=DATABASE) as session:
            
            # 1. Créer les Physical Application Components
            for app in entities['physical_applications']:
                query = """
                CREATE (app:PhysicalApplicationComponent {
                    id: $id,
                    nom: $nom,
                    acronyme: $acronyme,
                    type_domaine: $type_domaine,
                    description: $description,
                    type_application: $type_application,
                    criticite: $criticite,
                    statut: $statut,
                    utilisateurs: $utilisateurs,
                    frequence_utilisation: $frequence_utilisation,
                    frequence_releases: $frequence_releases,
                    hebergement: $hebergement,
                    strategie_cloud: $strategie_cloud,
                    technologies: $technologies,
                    cout_annuel: $cout_annuel,
                    maintenance_annuelle: $maintenance_annuelle,
                    commentaires: $commentaires
                })
                """
                session.run(query, **app)
            
            # 2. Créer les Business Capabilities
            for cap in entities['business_capabilities']:
                query = """
                CREATE (bc:BusinessCapability {
                    nom: $nom,
                    niveau_maturite: $niveau_maturite,
                    domaine: $domaine,
                    criticite_metier: $criticite_metier,
                    niveau_automatisation: $niveau_automatisation
                })
                """
                session.run(query, **cap)
            
            # 3. Créer les Business Services
            for service in entities['business_services']:
                query = """
                CREATE (bs:BusinessService {
                    nom: $nom,
                    type: $type,
                    domaine: $domaine,
                    sla: $sla,
                    cout_annuel: $cout_annuel
                })
                """
                session.run(query, **service)
            
            # 4. Créer les Business Functions
            for func in entities['business_functions']:
                query = """
                MERGE (bf:BusinessFunction {nom: $nom})
                SET bf.niveau = $niveau,
                    bf.budget_annuel = $budget_annuel,
                    bf.effectif = $effectif
                """
                session.run(query, **func)
            
            # 5. Créer les Data Entities
            for data in entities['data_entities']:
                query = """
                MERGE (de:DataEntity {nom: $nom})
                SET de.type = $type,
                    de.sensibilite = $sensibilite,
                    de.volume_quotidien = $volume_quotidien,
                    de.retention = $retention,
                    de.classification = $classification
                """
                session.run(query, **data)
            
            # 6. Créer les Technology Components
            for tech in entities['technology_components']:
                query = """
                MERGE (tc:PhysicalTechnologyComponent {nom: $nom})
                SET tc.version = $version,
                    tc.type = $type,
                    tc.provider = $provider,
                    tc.licence = $licence,
                    tc.support_fin = $support_fin
                """
                session.run(query, **tech)
            
            # 7. Créer les Vendors
            for vendor in entities['vendors']:
                query = """
                MERGE (v:Vendor {nom: $nom})
                SET v.type = $type,
                    v.criticite = $criticite,
                    v.pays = $pays,
                    v.contrat_fin = $contrat_fin,
                    v.satisfaction = $satisfaction
                """
                session.run(query, **vendor)
            
            logger.info("All entities created successfully")
    
    def create_togaf_relationships(self):
        """Crée les relations TOGAF entre les entités"""
        
        with self.driver.session(database=DATABASE) as session:
            
            # Relations Business Function -> Business Capability
            session.run("""
                MATCH (bf:BusinessFunction), (bc:BusinessCapability)
                WHERE bf.nom = bc.domaine
                CREATE (bf)-[:DELIVERS]->(bc)
            """)
            
            # Relations Business Capability -> Business Service
            session.run("""
                MATCH (bc:BusinessCapability), (bs:BusinessService)
                WHERE bc.domaine = bs.domaine
                CREATE (bc)-[:ENABLES]->(bs)
            """)
            
            # Relations Business Service -> Physical Application
            session.run("""
                MATCH (bs:BusinessService), (app:PhysicalApplicationComponent)
                WHERE bs.domaine = app.type_domaine
                CREATE (bs)-[:IS_SUPPORTED_BY]->(app)
            """)
            
            # Relations Application -> Technology
            session.run("""
                MATCH (app:PhysicalApplicationComponent), (tech:PhysicalTechnologyComponent)
                WHERE tech.nom IN app.technologies
                CREATE (app)-[:USES]->(tech)
            """)
            
            # Relations Application -> Data Entity
            session.run("""
                MATCH (app:PhysicalApplicationComponent), (de:DataEntity)
                WHERE app.type_domaine CONTAINS 'Finance' AND de.nom = 'Écriture Comptable'
                   OR app.type_domaine CONTAINS 'Risques' AND de.nom = 'Risque'
                   OR app.type_domaine CONTAINS 'Paiements' AND de.nom = 'Paiement'
                   OR app.type_domaine CONTAINS 'Distribution' AND de.nom = 'Client'
                   OR app.type_domaine CONTAINS 'Processus' AND de.nom = 'Document'
                CREATE (app)-[:PROCESSES]->(de)
            """)
            
            # Relations Application -> Vendor
            session.run("""
                MATCH (app:PhysicalApplicationComponent), (v:Vendor)
                WHERE (app.type_application CONTAINS 'Développement Interne' AND v.nom = 'EAI')
                   OR (app.type_application CONTAINS 'Progiciel' AND v.nom = split(app.nom, ' ')[0])
                CREATE (app)-[:IS_PROVIDED_BY]->(v)
            """)
            
            logger.info("All relationships created successfully")

def robust_read_csv(file):
    """Lecture robuste du fichier CSV avec différents encodages et séparateurs"""
    seps = [';', '\t', '|', ',']
    encodings = ['utf-8', 'latin1', 'cp1252']
    
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
    
    raise RuntimeError("Impossible de lire le CSV avec les encodages/séparateurs supportés.")

def process_csv_to_neo4j(file):
    """Fonction principale pour traiter un CSV et l'insérer dans Neo4j"""
    
    # Lire le CSV
    df = robust_read_csv(file)
    logger.info(f"CSV lu avec succès: {df.shape[0]} lignes, {df.shape[1]} colonnes")
    
    # Nettoyer les données
    df = df.dropna(how="all")
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.fillna("")
    
    # Initialiser le mapper
    mapper = TogafNeo4jMapper()
    
    try:
        # Nettoyer la base existante
        mapper.clear_database()
        
        # Créer les index
        mapper.create_indexes()
        
        # Mapper les données vers les entités TOGAF
        entities = mapper.map_csv_to_togaf_entities(df)
        
        # Insérer les entités
        mapper.insert_togaf_entities(entities)
        
        # Créer les relations
        mapper.create_togaf_relationships()
        
        logger.info("Traitement terminé avec succès")
        
        return {
            "status": "success",
            "message": f"Processed {df.shape[0]} applications into TOGAF Neo4j graph",
            "entities_created": {k: len(v) for k, v in entities.items()}
        }
        
    finally:
        mapper.close()

def get_graph_data():
    """Récupère les données du graphe pour visualisation"""
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    
    query = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN coalesce(n.nom, n.name) AS source,
           labels(n) AS source_labels,
           type(r) AS relation,
           coalesce(m.nom, m.name) AS target,
           labels(m) AS target_labels
    ORDER BY source
    """
    
    with driver.session(database=DATABASE) as session:
        result = session.run(query)
        data = []
        for record in result:
            if record["relation"] is None and record["target"] is None:
                continue
            data.append({
                "source": record["source"],
                "source_labels": record["source_labels"],
                "relation": record["relation"],
                "target": record["target"],
                "target_labels": record["target_labels"]
            })
    
    driver.close()
    return data
