# 5SPAR - Projet de construction d'une architecture Spark end-to-end pour les données Mastodon

Ici, vous trouverez toutes les informations concernant la capture des données en directes via un producer et Kafka.

## Producer — Ingestion des données Mastodon

Le **producer** constitue le point d'entrée des données dans notre architecture de streaming. 
Son rôle est de récupérer les toots depuis Mastodon et de les transmettre à **Kafka** sous forme de messages JSON.

L'ingestion est séparée en deux modes :

* **Backfill** : récupération de données historiques déjà publiées sur Mastodon.
* **Streaming** : récupération en temps réel des nouveaux toots publiés.

Les deux modes utilisent le même topic Kafka afin que le traitement en aval soit identique, quelle que soit l'origine du toot vu que c'est la même structure.

## Architecture générale

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

## Kafka