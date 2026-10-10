# 5SPAR — Projet de construction d'une architecture Spark end-to-end pour les données Mastodon

Vous trouverez ici toutes les informations concernant le spark streaming.

## 1. Présentation du projet

Le traitement est réalisé avec PySpark Structured Streaming. 
Les données reçues sont analysées, nettoyées et transformées avant d'être enregistrées dans une base de données SQL.

Le pipeline produit trois types de résultats :

- **Tweets transformés** : stockage des tweets en anglais après extraction des informations et nettoyage du contenu.
- **Statistiques horaires** : calcul du nombre approximatif de tweets par fenêtre d'une heure.
- **Statistiques sur les hashtags** : analyse des associations entre les tags du flux et les autres hashtags présents dans les tweets.

### Flux de données

1. Les messages Mastodon sont publiés dans un topic Kafka.
2. Spark lit les messages depuis Kafka en mode streaming.
3. Les messages JSON sont analysés à l'aide d'un schéma défini.
4. Les tweets en anglais sont conservés et leurs informations sont transformées.
5. Trois traitements indépendants produisent les résultats :
   - écriture des tweets transformés ;
   - calcul du nombre de tweets par fenêtre horaire ;
   - calcul des statistiques sur les hashtags.
6. Les résultats sont enregistrés dans la base de données SQL.

## 3. Fonctionnement du traitement PySpark

### 3.1. Lecture des données Kafka

Le pipeline utilise Structured Streaming pour consommer les messages d'un topic Kafka.

Les principales options sont :

- `startingOffsets = earliest` : lecture à partir des messages les plus anciens disponibles lorsque le point de départ est applicable.
- `maxOffsetsPerTrigger = 500` : limitation à 500 offsets maximum par micro-batch.
- `kafka.bootstrap.servers` : adresse du broker Kafka.
- `subscribe` : nom du topic Kafka à consommer.

Les paramètres de connexion sont définis dans le module :

`scripts.utils.mastodon_utils`

### 3.2. Parsing du JSON

Les messages Kafka sont reçus sous forme binaire, puis convertis en chaînes de caractères.

La fonction `from_json` permet d'extraire les informations à partir d'un schéma défini.

Les informations extraites comprennent :

- l'identifiant du tweet ;
- les dates de création et de modification ;
- le contenu textuel ;
- la langue ;
- les hashtags ;
- les informations du compte utilisateur ;
- le tag associé au flux.

Les métadonnées Kafka sont également conservées :

- `kafka_offset` ;
- `kafka_partition` ;
- `ingest_time`.

### 3.3. Filtrage et nettoyage

Le pipeline applique plusieurs transformations.

**Filtrage linguistique**

Seuls les tweets dont la langue est `en` sont conservés.

**Conversion des dates**

Les dates de création et de modification des tweets ainsi que la date de création des comptes sont converties au format timestamp.

Le jour et l'heure de création des tweets sont également extraits.

**Nettoyage du contenu**

Le texte des tweets est nettoyé afin de :

- supprimer les balises HTML ;
- remplacer certaines entités HTML ;
- supprimer les URL ;
- supprimer les hashtags présents dans le texte ;
- réduire les espaces multiples ;
- supprimer les espaces inutiles au début et à la fin du contenu.

Les hashtags structurés dans le JSON restent disponibles dans la colonne `tags_list`.

**Création d'un identifiant combiné**

Une colonne `id_stream_tweet_tag` est créée en concaténant l'identifiant du tweet et le tag du flux.

Cette colonne permet de conserver le contexte du flux associé au tweet.

## 4. Les trois traitements en streaming

### 4.1. Stockage des tweets transformés

Les tweets transformés sont enregistrés dans la table SQL :

`mastodon_stream`

L'écriture est réalisée avec `foreachBatch`, qui permet d'écrire chaque micro-batch dans la base de données via JDBC.

La fréquence de déclenchement est configurée à 10 secondes.

Les données stockées comprennent notamment :

- les identifiants des tweets ;
- les dates ;
- le contenu original et nettoyé ;
- les hashtags ;
- les informations des comptes ;
- les métadonnées Kafka ;
- le tag du flux.

### 4.2. Statistiques horaires

Les statistiques horaires sont enregistrées dans la table :

`mastodon_stream_windows`

Le traitement regroupe les tweets selon des fenêtres temporelles d'une heure.

Un watermark de deux minutes est appliqué sur la colonne `created_at`.

Pour chaque fenêtre, les indicateurs suivants sont calculés :

- `window_start` : début de la fenêtre ;
- `window_end` : fin de la fenêtre ;
- `created_at_hour` : heure de création des tweets ;
- `nb_tweets` : nombre approximatif de tweets distincts ;
- `ingest_time_windows` : date de génération du résultat.

Le calcul utilise `approx_count_distinct`, qui estime le nombre de tweets distincts.

Le traitement est déclenché toutes les 10 secondes.

### 4.3. Statistiques sur les hashtags

Les statistiques sont enregistrées dans la table :

`mastodon_stream_tags_statistics`

Le traitement extrait les hashtags de chaque tweet à partir de `tags_list`, puis associe chaque hashtag au `stream_tag` du message.

Le tag du flux lui-même est exclu des hashtags analysés.

Les données sont ensuite regroupées par :

- `stream_tag` ;
- `tag`.

Trois indicateurs sont calculés :

- `nb_tweets` : nombre approximatif de tweets distincts contenant le hashtag ;
- `nb_users` : nombre approximatif de comptes distincts associés au hashtag ;
- `avg_followers_count` : nombre moyen de followers des comptes associés au hashtag.

Une colonne `ingested_at` indique la date de génération des statistiques.

Cette requête utilise le mode `complete`, qui conserve l'ensemble des agrégats calculés par Spark.

L'écriture JDBC est toutefois configurée en mode `append`. Les résultats complets successifs peuvent donc s'accumuler dans la table SQL.

Le traitement est déclenché toutes les 10 minutes.

## 5. Checkpoints et reprise du streaming

Chaque requête possède son propre répertoire de checkpoint :

| Traitement | Répertoire |
|---|---|
| Tweets transformés | `${CHECKPOINT_DIR}/raw` |
| Fenêtres horaires | `${CHECKPOINT_DIR}/windows_hour` |
| Statistiques des hashtags | `${CHECKPOINT_DIR}/hashtag_stats` |

Les checkpoints permettent à Spark de conserver les informations nécessaires à la reprise des requêtes après un arrêt ou un redémarrage.

Ils doivent être persistants et accessibles par le processus Spark.

Attention : les checkpoints ne garantissent pas à eux seuls l'absence de doublons dans les tables SQL. 
Une stratégie d'écriture idempotente peut être nécessaire pour éviter les doublons lors des reprises.

### 6. Lancer le streaming Spark

Pour exécuter ce script, vous pouvez exécuter la commande suivante :

```bash
docker compose -f docker-compose-dev.yml exec streaming \
  python -m scripts.01_streaming_script
```