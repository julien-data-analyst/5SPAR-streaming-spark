import marimo

__generated_with = "0.25.1"
app = marimo.App()


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
    from mastodon_utils import get_spark, read_table, write_table, DATA_DIR
    #print("Fichiers de données :", os.listdir(DATA_DIR))
    return get_spark, read_table, write_table


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
def _(spark):
    from mastodon_utils import KAFKA_BOOTSTRAP, TOPIC
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", "mastodon_stream")
           .option("startingOffsets", "earliest")
           .load())
    raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset").show(5, truncate=80)
    print("OK - Kafka :", raw.count(), "messages dans le topic")
    return (TOPIC,)


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


@app.cell
def _(spark):
    spark.stop()
    return


if __name__ == "__main__":
    app.run()
