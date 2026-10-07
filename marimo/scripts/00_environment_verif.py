import marimo

__generated_with = "0.25.1"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    L'objectif de ce notebook est de vérifier que toute la configuration à la connexion de Spark avec ses librairies marchent dont vous pouvez retrouvez le build dans le Dockerfile de spark.
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Vérification de l'environnement MastodonStream
    Exécutez toutes les cellules. Chaque étape doit afficher **OK**.
    """)
    return


@app.cell
def _():
    import os
    from utils.mastodon_utils import get_spark, read_table, write_table, DATA_DIR, KAFKA_BOOTSTRAP, TOPIC
    print(f"Fichiers de données {DATA_DIR} commune :", os.listdir(DATA_DIR))
    return KAFKA_BOOTSTRAP, TOPIC, get_spark, read_table, write_table


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1. SparkSession (le premier lancement télécharge les connecteurs, ~1 min)
    """)
    return


@app.cell
def _(get_spark):
    spark = get_spark("spark-verif")
    print("OK - Spark", spark.version)
    return (spark,)


@app.cell
def _(spark):
    spark
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2. Connexion Kafka
    Nécessite que le topic `mastodon_stream` existe (Partie 1 du sujet). Lecture *batch* des messages déjà présents.
    """)
    return


@app.cell
def _(KAFKA_BOOTSTRAP, spark):
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", "mastodon_stream")
           .option("startingOffsets", "earliest")
           .load())
    raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset").show(5, truncate=80)
    print("OK - Kafka :", raw.count(), "messages dans le topic")
    return


@app.cell
def _(TOPIC):
    print(TOPIC)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Schéma à mettre en place :
    - key : string
    - value : JSON
    - partition : integer
    - offset : integer
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 3. Connexion PostgreSQL
    """)
    return


@app.cell
def _(read_table, spark, write_table):
    write_table(spark.createDataFrame([(1, "ok")], ["id", "status"]), "healthcheck", mode="overwrite")
    read_table(spark, "healthcheck").show()
    print("OK - PostgreSQL")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 4. Utilisation de batch avec le python worker du conteneur spark
    """)
    return


@app.cell
def _(spark):
    test = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)
        .load()
    )

    def test_batch(df, batch_id):
        print(f"===== BATCH {batch_id} =====")
        df.show(5, truncate=False)

    query = (
        test.writeStream
        .foreachBatch(test_batch)
        .option("checkpointLocation", "/tmp/test_foreachbatch")
        .trigger(processingTime="5 seconds")
        .start()
    )
    print("OK - Batch is working with python worker.")
    return


@app.cell
def _(spark):
    spark.stop() # Arrêter la session de config test
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Si tout est ok, vous pouvez passer à la suite des notebooks en commençant par :
    - le streaming (extraction, nettoyage, filtre, indicateurs statistiques)
    - le batch processing (aggrégation, indicateurs, etc)
    - l'analyse des sentiments et application sur les tweets futurs
    - la visualisation des données
    """)
    return


if __name__ == "__main__":
    app.run()
