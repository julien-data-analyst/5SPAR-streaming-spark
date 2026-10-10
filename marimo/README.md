# 5SPAR — Projet de construction d'une architecture Spark end-to-end pour les données Mastodon

Ici, vous trouverez toutes les informations concernant les différents notebooks créés pour le traitement batch, la visualisation graphique 
et la création du modèle d'analyse de sentiment.

# Ordre d'exécution des notebooks

Les notebooks du projet doivent être exécutés dans l'ordre suivant afin de vérifier d'abord l'environnement, puis de réaliser les différents traitements batch.

## 1. `00_environnement.py` — Vérification de l'environnement

Ce notebook permet de vérifier que l'environnement Spark est correctement configuré et que les différents composants nécessaires au projet sont accessibles.

Il vérifie notamment :

* la création et la connexion à une `SparkSession` ;
* la connexion à **Kafka** et la lecture du topic `mastodon_stream` ;
* la connexion à **PostgreSQL**, avec un test d'écriture et de lecture ;
* le fonctionnement de `foreachBatch` et du **Python Worker Spark**.

Le notebook crée également temporairement la table :

| Table SQL     | Utilisation                                                           |
| ------------- | --------------------------------------------------------------------- |
| `healthcheck` | Vérification de la lecture et de l'écriture entre Spark et PostgreSQL |

Cette table est uniquement utilisée comme **test de connexion** et ne constitue pas une table analytique du projet.

> **Objectif :** s'assurer que Spark peut communiquer correctement avec Kafka, PostgreSQL et l'environnement Python avant de lancer les traitements.

---

## 2. `02_batch_toots.py` — Agrégations statistiques sur les messages

Ce notebook réalise les traitements **batch orientés vers l'analyse des tweets/messages Mastodon**.

Les données sont lues depuis la table `mastodon_stream`, puis les doublons sont supprimés afin de travailler sur les tweets uniques.

Les principales analyses réalisées sont :

* nombre de tweets par **jour de la semaine** ;
* nombre de tweets par **mois** ;
* nombre d'**utilisateurs différents** ;
* extraction des **offres d'emploi** ;
* nombre total d'offres d'emploi ;
* répartition mensuelle des offres d'emploi ;
* calcul d'une **journée typique** à partir du nombre de tweets et d'offres par jour ;
* calcul d'indicateurs statistiques :

  * moyenne ;
  * premier quartile (Q1) ;
  * médiane ;
  * troisième quartile (Q3) ;
  * écart-type ;
* répartition du nombre de tweets selon le **nombre de stream tags associés**.

### Tables SQL créées

| Table SQL              | Contenu                                                                     |
| ---------------------- | --------------------------------------------------------------------------- |
| `day_of_week_tweets`   | Nombre de tweets par jour de la semaine                                     |
| `month_tweets`         | Nombre de tweets par mois                                                   |
| `jobs_tweets_mastodon` | Tweets identifiés comme des offres d'emploi, avec leurs tags et stream tags |
| `month_offer`          | Nombre d'offres d'emploi par mois                                           |
| `stream_tag_nb_tweets` | Nombre de tweets selon le nombre de stream tags associés                    |

Les résultats sont écrits dans PostgreSQL avec `overwrite`, afin de reconstruire les tables analytiques à partir des données disponibles. Par exemple, `day_of_week_tweets` est créé à partir de l'agrégation par jour de la semaine.

L'extraction des offres d'emploi est ensuite enregistrée dans `jobs_tweets_mastodon`.

> **Objectif :** produire les indicateurs statistiques généraux permettant d'analyser l'activité des messages Mastodon autour de la thématique de la Data.

---

## 3. `02_batch_tags.py` — Analyse des tags utilisés pour le streaming

Ce notebook réalise les traitements **batch orientés vers l'analyse des tags et des thématiques utilisées pour alimenter le streaming**.

Il permet notamment de calculer :

* le nombre de tweets associés à chaque **stream tag** ;
* les tags les plus fréquemment associés à chaque stream tag ;
* le nombre d'offres d'emploi associé à chaque stream tag ;
* le nombre d'utilisateurs différents par stream tag ;
* une **journée typique de tweets pour chaque stream tag** ;
* les statistiques sur le nombre de tweets et d'offres par journée :

  * moyenne ;
  * Q1 ;
  * médiane ;
  * Q3 ;
  * écart-type.

### Tables SQL créées

| Table SQL              | Contenu                                                  |
| ---------------------- | -------------------------------------------------------- |
| `tag_stream_nb_tweets` | Nombre de tweets pour chaque stream tag                  |
| `tag_couple_tweets`    | Nombre de tweets pour chaque couple `stream_tag` / `tag` |
| `tag_stream_jobs`      | Nombre d'offres d'emploi pour chaque stream tag          |
| `steam_tag_nb_users`   | Nombre d'utilisateurs différents pour chaque stream tag  |

Le notebook permet également de sélectionner un stream tag grâce à une interface interactive afin d'afficher ses statistiques journalières.

> **Objectif :** analyser les thématiques suivies par le pipeline de streaming et mesurer leur activité, leurs tags associés, leurs utilisateurs et leur présence dans les offres d'emploi.

## 3. `003_MLlib_sentiment.py` — Création du modèle d'analyse de sentiment

Ce notebook réalise le traitement batch pour la création du modèle via une méthode de régression logistique.

Cela se fera via deux fichiers CSV collectés de (kaggle)[https://www.kaggle.com/datasets/krishbaisoya/tweets-sentiment-analysis?select=train_data.csv]

Pour l'application, elle se fera sur la table SQL de nos tweets de la data capturé par le streaming.

Une partie visualisation graphique nous permets aussi d'observer le sentiment qu'apporte les textes concernant l'activité de la data (global et par tag).


> **Objectif :** création d'un modèle de prédiction de sentiment afin d'analyser le sentiment des contenus de tweets de la Data.

## 4. `02_batch_user_activity.py` — Analyse des comptes utilisateurs actifs dans le domaine de la data

Ce notebook réalise les traitements **batch orientés vers l'analyse des comptes utilisateurs et leurs tweets/tags associés**.

Il permet notamment de calculer :

* le top 10 des utilisateurs les plus actifs ;
* la répartition des activités des utilisateurs ;
* la longueur moyenne des publications des 10 utilisateurs les plus actifs ;

> **Objectif :** analyser les utilisateurs associées aux tweets du domaine Data et leurs activités

# Récapitulatif des tables SQL

| Notebook              | Table                  | Description                                |
| --------------------- | ---------------------- | ------------------------------------------ |
| `00_environnement.py` | `healthcheck`          | Test de connexion Spark ↔ PostgreSQL       |
| `02_batch_toots.py`   | `day_of_week_tweets`   | Tweets par jour de la semaine              |
| `02_batch_toots.py`   | `month_tweets`         | Tweets par mois                            |
| `02_batch_toots.py`   | `jobs_tweets_mastodon` | Tweets correspondant à des offres d'emploi |
| `02_batch_toots.py`   | `month_offer`          | Offres d'emploi par mois                   |
| `02_batch_toots.py`   | `stream_tag_nb_tweets` | Tweets selon le nombre de stream tags      |
| `02_batch_tags.py`    | `tag_stream_nb_tweets` | Tweets par stream tag                      |
| `02_batch_tags.py`    | `tag_couple_tweets`    | Tweets par couple stream tag / tag         |
| `02_batch_tags.py`    | `tag_stream_jobs`      | Offres d'emploi par stream tag             |
| `02_batch_tags.py`    | `steam_tag_nb_users`   | Utilisateurs par stream tag                |
| `03_MLlib_sentiment` | `mastodon_tweets_sentiment` | Tweets avec la prédiction du sentiment du contenu positif ou négatif |

## Ordre recommandé

```text
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
        ├── mastodon_tweets_sentiment
        │
        ▼
PostgreSQL
Tables analytiques
```

> **Remarque :** `healthcheck` est une table de test créée par `00_environnement.py`. Les autres tables constituent les résultats analytiques des traitements batch.
