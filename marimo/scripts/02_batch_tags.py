import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Dans ce notebook, vous verrez l'application de batch pour le calcul d'indicateurs sur l'axe des tags des tweets de Mastodon.

    Vous verrez notamment :
    - Le nombre de tweets par tag de stream
    - Par stream tag, compter les tags les plus populaires
    - Le nombre d'offre selon le tag du stream
    - Le nombre d'utilisateur par le tag du stream
    - Journée typique de tweets pour chaque tag
      - Moyenne du NB tweets
      - Moyenne du NB tweets offres
      - Q1 du NB de tweets
      - Médiane du NB de tweets
      - Q3 du NB de tweets
      - Q1 du NB de tweets d'offres
      - Médiane du NB de tweets d'offres
      - Q3 du NB de tweets d'offres
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Analyse statistique sur les tags associés aux tweets
    """)
    return


@app.cell
def _():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, LongType,
                                   BooleanType, ArrayType)
    from utils.mastodon_utils import (get_spark, read_table, write_table,
                                  KAFKA_BOOTSTRAP, TOPIC, CHECKPOINT_DIR, stop_all_streams, JDBC_PROPS, JDBC_URL, active_streams, write_result_into_console)

    spark = get_spark("batch-tags-mastodon", shuffle_partitions=4)
    return F, read_table, spark, write_table


@app.cell
def _(read_table, spark):
    toots_with_stream_tags = (
          read_table(spark, "mastodon_stream")
          .dropDuplicates(["id_stream_tweet_tag"]) # Supprimer les doublons éventuelles
    )
    return (toots_with_stream_tags,)


@app.cell
def _(toots_with_stream_tags):
    toots_with_stream_tags.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.1 Le nombre de tweets par tag utilisé pour le streaming
    """)
    return


@app.cell
def _(F, toots_with_stream_tags):
    toots_nb_by_tag = (
        toots_with_stream_tags
        .groupBy("stream_tag")
        .agg(F.count("id_stream_tweet_tag").alias("nb_tweets"))
    )
    return (toots_nb_by_tag,)


@app.cell
def _(toots_nb_by_tag):
    toots_nb_by_tag.show()
    return


@app.cell
def _(toots_nb_by_tag, write_table):
    # Ecriture du résultat dans une table analytique SQL
    write_table(toots_nb_by_tag, "tag_stream_nb_tweets", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.2 Table analytique pour compter par tag stream le nombre de tweets
    """)
    return


@app.cell
def _(F, toots_with_stream_tags):
    toots_stream_tag_by_tag = (
        toots_with_stream_tags
        .select("stream_tag", F.explode("tags_list").alias("tag"))
        .filter(F.col("stream_tag") != F.col("tag"))
        .groupBy("stream_tag", "tag")
        .agg(F.count("stream_tag").alias("nb_tweets"))
    )
    return (toots_stream_tag_by_tag,)


@app.cell
def _(toots_stream_tag_by_tag):
    toots_stream_tag_by_tag.show()
    return


@app.cell
def _(toots_stream_tag_by_tag, write_table):
    # Ecriture du résultat dans une table analytique SQL
    write_table(toots_stream_tag_by_tag, "tag_couple_tweets", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.3 Le nombre d'offre par tag stream
    """)
    return


@app.cell
def _(F, read_table, spark):
    data_jobs = (
          read_table(spark, "jobs_tweets_mastodon")
          .select("tweet_id", F.explode("stream_tags").alias("tag"))
          .groupBy("tag")
          .agg(F.count("tag").alias("nb_tweets"))
    )
    return (data_jobs,)


@app.cell
def _(data_jobs):
    data_jobs.show()
    return


@app.cell
def _(data_jobs, write_table):
    # Ecriture du résultat dans une table analytique SQL
    write_table(data_jobs, "tag_stream_jobs", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.4 Le nombre d'utilisateurs par tag
    """)
    return


@app.cell
def _(F, toots_with_stream_tags):
    user_by_tags = (
          toots_with_stream_tags
          .select("account_id", "stream_tag")
          .groupBy("stream_tag")
          .agg(F.count_distinct("account_id").alias("nb_users"))
    )
    return (user_by_tags,)


@app.cell
def _(user_by_tags):
    user_by_tags.show()
    return


@app.cell
def _(user_by_tags, write_table):
    write_table(user_by_tags, "steam_tag_nb_users", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.5 Journée typique de tweets par stream_tag
    """)
    return


@app.cell
def _(F, toots_with_stream_tags):
    typic_day_tags = (
        toots_with_stream_tags
            .select(F.to_date("created_at").alias("date"), "tweet_id", "stream_tag", F.explode("tags_list").alias("tag"))
            .select("*", (F.col("tag").like("%job%") | F.col("tag").like("hiring")).alias("job")) 
            #.filter("job = True")
             .groupBy("stream_tag", "date")
             .agg(F.count_distinct("tweet_id").alias("nb_tweets"), F.count_if(F.col("job")).alias("nb_tweets_offre"))
    )
    return (typic_day_tags,)


@app.cell
def _(typic_day_tags):
    typic_day_tags.show()
    return


@app.cell(hide_code=True)
def _(mo, toots_with_stream_tags):
    dropdown = mo.ui.dropdown(toots_with_stream_tags.select("stream_tag").distinct().toPandas()["stream_tag"].values, value="datascience", allow_select_none=False)

    mo.md(f"""
        **Choissisez la thématique générale que vous voulez observer :**

        {dropdown}

        """)
    return (dropdown,)


@app.cell
def _(F, dropdown, typic_day_tags):
    daily_tweets_stats_choose_tag = typic_day_tags.filter(f"stream_tag = '{dropdown.value}'").select(
        # Calcul des indicateurs de statistiques pour le nombre de tweets dans une journée
        F.mean("nb_tweets").alias("moyenne_nb_tweets"),
        F.expr("percentile(nb_tweets, 0.25)").alias("Q1_nb_tweets"),
        F.expr("percentile(nb_tweets, 0.50)").alias("mediane_nb_tweets"),
        F.expr("percentile(nb_tweets, 0.75)").alias("Q3_nb_tweets"),
        F.stddev("nb_tweets").alias("ecart_type_nb_tweets"),
    
        # Calcul des indicateurs de statistique pour le nombre d'offre dans une journée
        F.mean("nb_tweets_offre").alias("moyenne_nb_tweets_offre"),
        F.expr("percentile(nb_tweets_offre, 0.25)").alias("Q1_nb_tweets_offre"),
        F.expr("percentile(nb_tweets_offre, 0.50)").alias("mediane_nb_tweets_offre"),
        F.expr("percentile(nb_tweets_offre, 0.75)").alias("Q3_nb_tweets_offre"),
        F.stddev("nb_tweets_offre").alias("ecart_type_nb_tweets_offre")

    )
    return (daily_tweets_stats_choose_tag,)


@app.cell
def _(daily_tweets_stats_choose_tag, dropdown, mo):
    stats = daily_tweets_stats_choose_tag.first()

    markdown_stats = f"""

    ## Pour le tag du {dropdown.value}

    ### 📊 Statistiques sur le nombre d'offres d'emplois par journée

    - **25 %** des journées comptent au maximum **{stats["Q1_nb_tweets_offre"]:.0f}** offres d'emplois.
    - **50 %** des journées comptent au maximum **{stats["mediane_nb_tweets_offre"]:.0f}** offres d'emplois.
    - **75 %** des journées comptent au maximum **{stats["Q3_nb_tweets_offre"]:.0f}** offres d'emplois.

    La **moyenne** est de **{stats["moyenne_nb_tweets_offre"]:.2f}** offres d'emplois par journée, avec un **écart-type** de **{stats["ecart_type_nb_tweets_offre"]:.2f}**.

    ### 📊 Statistiques sur le nombre de tweets par journée

    - **25 %** des journées comptent au maximum **{stats["Q1_nb_tweets"]:.0f}** de tweets.
    - **50 %** des journées comptent au maximum **{stats["mediane_nb_tweets"]:.0f}** de tweets.
    - **75 %** des journées comptent au maximum **{stats["Q3_nb_tweets"]:.0f}** de tweets.

    La **moyenne** est de **{stats["moyenne_nb_tweets"]:.2f}** tweets par journée, avec un **écart-type** de **{stats["ecart_type_nb_tweets"]:.2f}**.
    """

    mo.md(markdown_stats)
    return


@app.cell
def _():
    #spark.stop()
    return


if __name__ == "__main__":
    app.run()
