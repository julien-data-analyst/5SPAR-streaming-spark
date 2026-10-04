# 5SPAR - Projet de construction d'une architecture Spark end-to-end pour les données Mastodon

Ici, vous trouverez toutes les informations concernant la capture des données en directes via un producer et Kafka.

## Producer — Ingestion des données Mastodon

Le **producer** constitue le point d'entrée des données dans notre architecture de streaming. 
Son rôle est de récupérer les toots depuis Mastodon et de les transmettre à **Kafka** sous forme de messages JSON.

L'ingestion est séparée en deux modes :

* **Backfill** : récupération de données historiques déjà publiées sur Mastodon.
* **Streaming** : récupération en temps réel des nouveaux toots publiés.

Les deux modes utilisent le même topic Kafka afin que le traitement en aval soit identique, quelle que soit l'origine du toot vu que c'est la même structure.

### Architecture générale

```text
                         MASTODON
                             │
                ┌────────────┴────────────┐
                │                         │
                │                         │
          REST API                    Streaming API
          historique                  temps réel
                │                         │
                ▼                         ▼
          ┌───────────┐             ┌────────────┐
          │ backfill  │             │ streaming  │
          │   .py     │             │    .py     │
          └─────┬─────┘             └──────┬─────┘
                │                          │
                │       JSON des toots     │
                └────────────┬─────────────┘
                             ▼
                     ┌──────────────┐
                     │     Kafka    │
                     │              │
                     │ mastodon_    │
                     │ stream       │
                     └───────┬──────┘
                             │
                             │ Structured Streaming
                             ▼
                     ┌──────────────┐
                     │     Spark    │
                     │   Streaming  │
                     └───────┬──────┘
                             │
                             │ données
                             │ nettoyées /
                             │ transformées
                             ▼
                     ┌──────────────┐
                     │ PostgreSQL   │
                     └──────────────┘
```

### Commandes pour lancer le producer

Ici, vous allez voir les différentes commandes pour lancer la collecte des données liées aux hashtags.

#### 1. Mode Backfill

Le mode backfill permet de récupérer l'historique des toots associés aux hashtags configurés.

Par défaut, le backfill remonte jusqu'au :

```
2026-01-01
```

Commande par défaut :

```
docker compose -f docker-compose-dev.yml exec python -m scripts.main --mode backfill

docker compose -f docker-compose-prod.yml exec python -m scripts.main --mode backfill

```

Cette commande utilise la date limite par défaut :

```
--until 2026-01-01
```

La date peut être modifiée avec l'argument --until au format YYYY-MM-DD.

#### 2. Mode Streaming

Le mode stream permet de récupérer les nouveaux toots en temps réel pour les hashtags configurés.

Commande :
```

docker compose -f docker-compose-dev.yml exec python -m scripts.main --mode stream 

docker compose -f docker-compose-prod.yml exec python -m scripts.main -mode stream

```

## Kafka - capturer les données de notre producer

Ici, vous allez voir les différentes commandes pour intéragir avec Kafka qui capture les données.

### 1. Vérifier que Kafka fonctionne

```
docker compose -f docker-compose-dev.yml ps
```

Entrer dans le conteneur Kafka :

```
docker compose -f docker-compose-dev.yml exec kafka bash
```

### 2. Lister les topics

```
docker compose -f docker-compose-dev.yml exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

### 3. Créer le topic mastodon_stream

```
docker compose -f docker-compose-dev.yml exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic mastodon_stream \
  --partitions 3 \
  --replication-factor 1
```

Pour un environnement de développement avec un seul broker, --replication-factor 1 est normal.

### 4. Vérifier les informations du topic

```
docker compose -f docker-compose-dev.yml exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic mastodon_stream
```