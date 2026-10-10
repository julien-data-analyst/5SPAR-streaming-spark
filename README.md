# 5SPAR - Projet de construction d'une architecture Spark end-to-end pour les données Mastodon
## Auteur : Julien RENOULT - Béatrice BEAVOGUI
## Promo : SUPINFO Programme Grande École 5ème année
### *Spécialité : Ingénierie Data*
### *Date : 03/10/2026-10/10/2026*
### Lien Projet : https://github.com/julien-data-analyst/5SPAR-streaming-spark

# Introduction

Dans le cadre de ce projet, nous devions construire une architecture Spark de bout en bout 
pour capturer des données en streaming avec l'[API Mastodon](https://docs.joinmastodon.org/client/intro/).

Pour réaliser ce projet, plusieurs technologies devront être utilisées :
- **Kafka** : pour la capture en temps réel des données
- **Spark** : pour traiter les données en streaming et en batch avec la possibilité de faire du machiner learning
- **PostgreSQL** : pour stocker les données de streaming traitées pour l'historisation
- **Python** : pour l'utilisation de Spark dans ses différentes couches (streaming, etc) et création de visualisations graphiques

Pour chaque dossier du projet contiendra un README.md détaillé sur comment est utilisé la technologie concernée, 
les commandes à utiliser pour faire telle action et l'explication en générale de comment est construit la fonctionnalité


## ⚙️ Pré-requis

Avant de lancer le projet, veuillez vérifier que vous avez bien installé :

* Docker
* Docker Compose

---

## 🛠️ Lancer le projet

Deux docker compose sont disponibles pour lancer les conteneurs.

### Development Environment

Commencer l'application :

```bash
docker compose -f docker-compose-dev.yml up --build
```

Arrête l'application :

```bash
docker compose -f docker-compose-dev.yml down
```

Attention dans la version dev, il faudra lancer manuellement le spark streaming, la création du topic, le producer en mode backfill et streaming :

#### Créer le topic Kafka 

```bash

docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic "${KAFKA_TOPIC:-mastodon_stream}" \
  --partitions "${KAFKA_PARTITIONS:-1}" \
  --replication-factor "${KAFKA_REPLICATION_FACTOR:-1}"

```

Pour vérifier s'il a bien été créé :

```bash

docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

#### Lancer le producer mastodon

En mode streaming : 

```bash

docker compose -f docker-compose-dev.yml exec producer \
  python -m scripts.main --mode stream
```

En mode backfill :

```bash
docker compose -f docker-compose-prod.yml exec producer \
  python -m scripts.main --mode backfill

```

#### Lancer le spark streaming 

```bash

docker compose -f docker-compose-dev.yml exec streaming \
  python -m scripts.01_streaming_script
```

### Production Environment

Commencer l'application :

```bash
docker compose -f docker-compose-prod.yml up --build
```

Arrêter l'application :

```bash
docker compose -f docker-compose-prod.yml down
```

---

# Ordre d'exécution des notebooks

Les notebooks du projet doivent être exécutés dans l'ordre suivant afin de vérifier d'abord l'environnement, 
puis de réaliser les différents traitements batch avec la création, l'application du modèle créé.

## Ordre recommandé

```text
00_environnement.py
        │
        │ Vérification de Spark, Kafka,
        │ PostgreSQL et Python Worker
        ▼
02_batch_toots.py
        │
        │ Agrégations statistiques
        │ sur les messages Mastodon
        │
        ├── day_of_week_tweets
        ├── month_tweets
        ├── jobs_tweets_mastodon
        ├── month_offer
        └── stream_tag_nb_tweets
        │
        ▼
02_batch_tags.py
        │
        │ Analyse des stream tags
        │ et des tags associés
        │
        ├── tag_stream_nb_tweets
        ├── tag_couple_tweets
        ├── tag_stream_jobs
        └── steam_tag_nb_users
        │
        ▼
02_batch_user_activity.py
        │
        │ Analyse des activités des utilisateur
        │ associés à leurs tweets
        │
        │
        ▼
03_MLlib_sentiment.py
        │
        │ Construction du modèle 
        │ et des tags associés
        │
        ├── tag_stream_nb_tweets
        ├── tag_couple_tweets
        ├── tag_stream_jobs
        └── steam_tag_nb_users
        │
        ▼
PostgreSQL
Tables analytiques
```

# Schéma du pipeline

## Présentation générale

Le pipeline de traitement des données Mastodon repose sur une architecture combinant ingestion, 
traitement distribué en temps réel, stockage relationnel et analyse batch.

Les messages sont collectés depuis l'API Mastodon par un producteur Python, 
puis envoyés au format JSON dans un topic Kafka, avec le `toot_id` comme clé de message. 

Kafka assure la transmission, la conservation des données entre le producteur et le traitement temps réel de spark.

Spark Structured Streaming consomme ensuite les messages, extrait les données JSON, 
effectue leur nettoyage et calcule des statistiques sur les hashtags. Le traitement utilise des fenêtres temporelles d'une heure et un watermark de deux minutes pour gérer les retards d'arrivée des données. Les résultats sont écrits dans PostgreSQL via JDBC, notamment au moyen de `foreachBatch`.

En parallèle, des données CSV ont été téléchargées sur 
(Kaggle)[https://www.kaggle.com/datasets/krishbaisoya/tweets-sentiment-analysis?select=train_data.csv]
spécifiquement pour l'entraînement d'un modèle d'analyse de sentiment.

## Architecture du pipeline

```mermaid
flowchart TD

    subgraph SRC["1. Source des données"]
        A["API Mastodon<br/>Collecte des publications<br/>par stream tag"]
        H["Fichiers CSV historiques<br/>Données pour l'analyse<br/>et la modélisation"]
    end

    subgraph ING["2. Ingestion des données"]
        B["Producer Python<br/>Collecte et sérialisation JSON<br/>Clé Kafka : toot_id"]
        C[("Kafka<br/>Topic : mastodon_stream<br/>3 partitions")]
    end

    subgraph STR["3. Traitement en temps réel"]
        D["Spark Structured Streaming<br/>Lecture du flux Kafka<br/>Parsing JSON et nettoyage"]
        W["Agrégation temporelle<br/>Fenêtres de 1 heure<br/>Watermark de 2 minutes<br/>Statistiques par hashtag"]
    end

    subgraph STO["4. Stockage des données"]
        E[("PostgreSQL<br/>mastodon_stream<br/>mastodon_stream_tags_statistics<br/>mastodon_stream_windows")]
        J[("CSV<br/>Données CSV pour le Machine Learning")]
    end

    subgraph BAT["5. Traitement batch et analyse"]
        F["Spark batch<br/>Lecture JDBC<br/>Jointures et agrégations"]
        G["Préparation des données<br/>Features et variables explicatives"]
        M["Modélisation<br/>Entraînement et application<br/>Spark MLlib / Python"]
    end

    subgraph OUT["6. Restitution"]
        I["Résultats analytiques<br/>Statistiques et prédictions"]
        V["Visualisations<br/>Graphiques réalisés sous seaborn et plotnine"]
    end

    A -->|"Requêtes API"| B
    B -->|"Messages JSON"| C
    C -->|"readStream"| D
    D --> W
    W -->|"foreachBatch / JDBC"| E

    H -->|"Chargement CSV"| J

    E -->|"Lecture JDBC"| F
    J -->|"Lecture JDBC"| F

    F --> G
    G --> M
    M --> I
    I --> V
```

## Légende

| Étape | Composant | Rôle |
|---|---|---|
| 1 | API Mastodon | Source des publications collectées selon les stream tags ciblés. |
| 2 | Producer Python | Collecte les publications, les sérialise au format JSON et les publie dans Kafka avec `toot_id` comme clé. |
| 2 | Kafka | Assure le découplage entre ingestion et traitement, conserve les messages selon sa configuration et distribue les données entre les partitions du topic. |
| 3 | Spark Structured Streaming | Consomme les messages Kafka, interprète le JSON, nettoie les données et calcule des statistiques temporelles sur les hashtags. |
| 3 | Fenêtres et watermark | Regroupe les données sur des fenêtres d'une heure et définit une tolérance de deux minutes pour les événements tardifs, selon la configuration du traitement. |
| 4 | PostgreSQL | Stocke les données nettoyées, les statistiques du flux et les données historiques utilisées pour les analyses. |
| 5 | Spark batch | Réalise les jointures, les agrégations et les transformations nécessaires à l'analyse approfondie. |
| 5 | Spark MLlib / Python | Prépare les variables, entraîne les modèles et effectue les prédictions selon les besoins du projet. |
| 6 | Visualisations | Présente les statistiques et les résultats des modèles sous forme de graphiques et de tableaux de bord. |
