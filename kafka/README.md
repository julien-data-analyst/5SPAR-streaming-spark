# 5SPAR — Projet de construction d'une architecture Spark end-to-end pour les données Mastodon

Vous trouverez ici toutes les informations concernant la capture des données Mastodon en direct via un producer et Kafka.

## Producer — Ingestion des données Mastodon

Le **producer** constitue le point d'entrée des données dans notre architecture de streaming.

Son rôle est de récupérer les toots depuis Mastodon et de les transmettre à **Kafka** sous forme de messages JSON.

L'ingestion est séparée en deux modes :

* **Backfill** : récupération de données historiques déjà publiées sur Mastodon.
* **Streaming** : récupération en temps réel des nouveaux toots publiés.

Les deux modes utilisent le même topic Kafka afin que le traitement en aval soit identique, quelle que soit l'origine du toot, puisque les données utilisent la même structure JSON.

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
                     │    Spark     │
                     │   Streaming  │
                     └───────┬──────┘
                             │
                             │ Données nettoyées
                             │ et transformées
                             ▼
                     ┌──────────────┐
                     │ PostgreSQL   │
                     └──────────────┘
```

---

## Commandes pour lancer le producer

Les commandes suivantes permettent de lancer la collecte des données associées aux hashtags configurés dans le producer.

### 1. Mode Backfill

Le mode **backfill** permet de récupérer l'historique des toots associés aux hashtags configurés.

Par défaut, le backfill remonte jusqu'à la date :

```text
2026-01-01
```

#### Commande par défaut

En environnement de développement :

```bash
docker compose -f docker-compose-dev.yml exec producer \
  python -m scripts.main --mode backfill
```

En environnement de production :

```bash
docker compose -f docker-compose-prod.yml exec producer \
  python -m scripts.main --mode backfill
```

Cette commande utilise implicitement :

```text
--until 2026-01-01
```

#### Modifier la date limite

La date limite peut être modifiée avec l'argument `--until`, au format `YYYY-MM-DD`.

Exemple :

```bash
docker compose -f docker-compose-dev.yml exec producer \
  python -m scripts.main \
  --mode backfill \
  --until 2026-01-01
```

Dans cet exemple, le producer récupère les données historiques jusqu'à la date limite définie.

---

### 2. Mode Streaming

Le mode **streaming** permet de récupérer les nouveaux toots en temps réel pour les hashtags configurés.

#### Environnement de développement

```bash
docker compose -f docker-compose-dev.yml exec producer \
  python -m scripts.main --mode stream
```

#### Environnement de production

```bash
docker compose -f docker-compose-prod.yml exec producer \
  python -m scripts.main --mode stream
```

Contrairement au mode backfill, l'argument `--until` n'est pas utilisé en mode streaming.

Le producer reste actif afin de recevoir les nouveaux toots publiés sur les hashtags surveillés et de les transmettre à Kafka.

---

# Kafka — Capture des données du producer

Kafka reçoit les messages JSON produits par le producer Mastodon dans le topic :

```text
mastodon_stream
```

Les commandes suivantes permettent d'administrer le topic et d'observer les messages reçus.

> Les exemples ci-dessous utilisent l'environnement de développement. Pour l'environnement de production, remplacer `docker-compose-dev.yml` par `docker-compose-prod.yml`.

## 1. Vérifier que Kafka fonctionne

```bash
docker compose -f docker-compose-dev.yml ps
```

Pour accéder directement au conteneur Kafka :

```bash
docker compose -f docker-compose-dev.yml exec kafka bash
```

---

## 2. Lister les topics

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

Cette commande permet notamment de vérifier que le topic `mastodon_stream` existe.

---

## 3. Créer le topic `mastodon_stream`

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic mastodon_stream \
  --partitions 3 \
  --replication-factor 1
```

Le topic est configuré avec :

* **3 partitions** ;
* **1 replica** par partition.

Dans un environnement de développement avec un seul broker Kafka, `--replication-factor 1` est normal.

---

## 4. Vérifier les informations du topic

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic mastodon_stream
```

Cette commande permet notamment de consulter :

* le nombre de partitions ;
* le leader de chaque partition ;
* les replicas ;
* les ISR (*In-Sync Replicas*).

---

## 5. Supprimer un topic

> ⚠️ Cette commande supprime le topic et les messages qu'il contient.

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --delete \
  --topic mastodon_stream
```

Puis vérifier sa suppression :

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

---

# Consommer les messages Kafka

## 6. Créer un consommateur Kafka

Un consommateur peut être associé à un **consumer group** grâce à l'option `--group`.

Exemple :

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic mastodon_stream \
  --group mastodon-debug
```

Le terminal reste ouvert et affiche les nouveaux messages consommés depuis Kafka.

Le consumer group `mastodon-debug` permet notamment à Kafka de mémoriser la position de lecture du consommateur.

---

## 7. Consommer les messages depuis le début

Pour observer les messages déjà présents dans le topic :

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic mastodon_stream \
  --from-beginning
```

Cette commande permet notamment de vérifier les données envoyées lors du **backfill**.

---

## 8. Afficher également la clé Kafka

Le producer utilise l'identifiant du toot comme clé Kafka :

```python
key=toot_id
```

Il est donc possible d'afficher cette clé avec :

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic mastodon_stream \
  --from-beginning \
  --property print.key=true
```

Le résultat peut ressembler à :

```text
123456789123456789    {"stream_tag":"dataengineering","status":{...}}
```

La première valeur correspond à la clé Kafka et la seconde au message JSON.

---

## 9. Afficher la clé, la partition et l'offset

Pour observer plus précisément le fonctionnement de Kafka :

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic mastodon_stream \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

On peut alors obtenir un résultat similaire à :

```text
Partition:0    Offset:42    Key:123456789    {"stream_tag":"dataengineering",...}
```

Cela permet notamment d'observer :

* la **partition** dans laquelle le message est stocké ;
* son **offset** dans cette partition ;
* sa **clé Kafka** ;
* son contenu JSON.

---

# Consumer groups

## 10. Lister les consumer groups

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --list
```

---

## 11. Consulter l'état d'un consumer group

Pour observer le groupe `mastodon-debug` :

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --group mastodon-debug
```

Cette commande permet notamment de voir :

* le dernier offset consommé ;
* le dernier offset disponible ;
* le **lag** du consommateur.

Exemple :

```text
GROUP           TOPIC            PARTITION   CURRENT-OFFSET   LOG-END-OFFSET   LAG
mastodon-debug  mastodon_stream  0           120              125              5
mastodon-debug  mastodon_stream  1           98               98               0
mastodon-debug  mastodon_stream  2           104              110              6
```

Le **lag** correspond au nombre de messages encore disponibles dans Kafka mais qui n'ont pas encore été consommés par le consumer group.

---

# Vérification complète du pipeline

Pour vérifier que toute la chaîne fonctionne, il est possible d'utiliser plusieurs terminaux.

### Terminal 1 — Producer Mastodon

```bash
docker compose -f docker-compose-dev.yml exec producer \
  python -m scripts.main --mode stream
```

Le producer récupère alors les nouveaux toots depuis Mastodon.

### Terminal 2 — Consumer Kafka

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic mastodon_stream \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

Ce terminal permet d'observer les messages arrivant dans Kafka.

### Terminal 3 — Vérification du topic

```bash
docker compose -f docker-compose-dev.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic mastodon_stream
```

On peut ainsi vérifier simultanément :

```text
Mastodon
   │
   │ Streaming API
   ▼
Producer
   │
   │ KafkaProducer
   ▼
Kafka
   │
   │ mastodon_stream
   ▼
Kafka Consumer
   │
   ▼
Vérification des messages
```

Une fois cette étape validée, 
le consumer Kafka de test pourra être remplacé par **Spark Structured Streaming**, qui assurera le traitement des données avant leur insertion dans PostgreSQL.
