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
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, LongType,
                                   BooleanType, ArrayType)
    from utils.mastodon_utils import (get_spark, read_table, write_table,
                                  KAFKA_BOOTSTRAP, TOPIC, CHECKPOINT_DIR, stop_all_streams, JDBC_PROPS, JDBC_URL, active_streams, write_result_into_console)
    from plotnine import (
        ggplot,
        aes,
        geom_col,
        geom_text,
        coord_flip,
        facet_wrap,
        labs,
        theme_minimal,
        theme,
        element_text
    )

    spark = get_spark("batch-tags-mastodon", shuffle_partitions=4)
    return (
        F,
        Window,
        aes,
        coord_flip,
        element_text,
        facet_wrap,
        geom_col,
        geom_text,
        ggplot,
        labs,
        pd,
        read_table,
        spark,
        theme,
        theme_minimal,
        write_table,
    )


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
    Faisons maintenant la visualisation graphique.
    """)
    return


@app.cell
def _(F, pd, toots_nb_by_tag):
    top_10_tags = (
        toots_nb_by_tag
        .orderBy(F.desc("nb_tweets"))
        .limit(10)
    )

    # Conversion en Pandas pour plotnine
    top_10_tags_pd = top_10_tags.toPandas()

    # Ordonner les catégories pour le graphique
    top_10_tags_pd["stream_tag"] = pd.Categorical(
        top_10_tags_pd["stream_tag"],
        categories=top_10_tags_pd.sort_values(
            "nb_tweets"
        )["stream_tag"].tolist(),
        ordered=True
    )
    return (top_10_tags_pd,)


@app.cell
def _(
    aes,
    coord_flip,
    element_text,
    geom_col,
    geom_text,
    ggplot,
    labs,
    theme,
    theme_minimal,
    top_10_tags_pd,
):
    graph_top_10_tags = (
        ggplot(
            top_10_tags_pd,
            aes(
                x="stream_tag",
                y="nb_tweets"
            )
        )
        + geom_col(fill="#377EB8", width=0.7)
        + geom_text(
            aes(label="nb_tweets"),
            ha="left",
            nudge_y=0.3,
            size=9
        )
        + coord_flip()
        + labs(
            title="Top 10 des tags les plus utilisés",
            subtitle="Nombre de publications par tag",
            x="Tag",
            y="Nombre de publications"
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 7),
            plot_title=element_text(size=16, weight="bold"),
            axis_text=element_text(size=10)
        )
    )

    graph_top_10_tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Commentaire :

    On observe dans le top 3 des tags utilisés pour la capture de tweets sont **dataviz, dataengineering et bigdata** avec dataviz qui a à lui seul plus de 1 500 tweets le concernant.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.2 Table analytique pour compter par couple de tags le nombre de tweets
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
    Faisons maintenant la visualisation graphique en observant les trois tags les plus utilisés par stream tag.
    """)
    return


@app.cell
def _(F, Window, toots_stream_tag_by_tag):
    # Avec windows, cela permets de donner un rang à nos tags par stream_tag et de filtrer sur les tags les plus utilisés
    window_tags = (
        Window
        .partitionBy("stream_tag")
        .orderBy(
            F.desc("nb_tweets"),
            F.asc("tag")
        )
    )

    top_3_by_stream_tag = (
        toots_stream_tag_by_tag
        .withColumn(
            "rang",
            F.row_number().over(window_tags)
        )
        .filter(F.col("rang") <= 3)
    )

    top_3_by_stream_tag_pd = top_3_by_stream_tag.toPandas()
    return (top_3_by_stream_tag_pd,)


@app.cell
def _(
    aes,
    element_text,
    facet_wrap,
    geom_col,
    geom_text,
    ggplot,
    labs,
    theme,
    theme_minimal,
    top_3_by_stream_tag_pd,
):
    graph_top_3_by_stream_tag = (
        ggplot(
            top_3_by_stream_tag_pd,
            aes(
                x="tag",
                y="nb_tweets"
            )
        )
        + geom_col(fill="#4DAF4A", width=0.7)
        + geom_text(
            aes(label="nb_tweets"),
            va="bottom",
            nudge_y=0.3,
            size=8
        )
        + facet_wrap(
            "~stream_tag",
            scales="free"
        )
        + labs(
            title="Top 3 des tags par stream_tag",
            subtitle="Les trois tags les plus fréquents dans chaque catégorie",
            x="Tag associé",
            y="Nombre de publications"
        )
        + theme_minimal()
        + theme(
            figure_size=(14, 9),
            plot_title=element_text(size=16, weight="bold"),
            axis_text_x=element_text(
                rotation=45,
                ha="right",
                size=8
            ),
            strip_text=element_text(
                size=10,
                weight="bold"
            )
        )
    )

    graph_top_3_by_stream_tag
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
    Faisons maintenant la visualisation graphique.
    """)
    return


@app.cell
def _(F, user_by_tags):
    top_10_users_by_tag = (
        user_by_tags
        .orderBy(
            F.desc("nb_users"),
            F.asc("stream_tag")
        )
        .limit(10)
    )

    top_10_users_by_tag_pd = top_10_users_by_tag.toPandas()
    return (top_10_users_by_tag_pd,)


@app.cell
def _(top_10_users_by_tag_pd):
    top_10_users_by_tag_pd
    return


@app.cell
def _(
    aes,
    coord_flip,
    element_text,
    geom_col,
    geom_text,
    ggplot,
    labs,
    pd,
    theme,
    theme_minimal,
    top_10_users_by_tag_pd,
):
    top_10_users_by_tag_pd["stream_tag"] = pd.Categorical(
        top_10_users_by_tag_pd["stream_tag"],
        categories=top_10_users_by_tag_pd.sort_values(
            "nb_users"
        )["stream_tag"].tolist(),
        ordered=True
    )

    graph_top_10_users_by_tag = (
        ggplot(
            top_10_users_by_tag_pd,
            aes(
                x="stream_tag",
                y="nb_users"
            )
        )
        + geom_col(fill="#984EA3", width=0.7)
        + geom_text(
            aes(label="nb_users"),
            ha="left",
            nudge_y=0.3,
            size=9
        )
        + coord_flip()
        + labs(
            title="Top 10 des tags par nombre d'utilisateurs",
            subtitle="Nombre de comptes distincts ayant utilisé chaque tag",
            x="Tag",
            y="Nombre d'utilisateurs"
        )
        + theme_minimal()
        + theme(
            figure_size=(12, 7),
            plot_title=element_text(size=16, weight="bold"),
            axis_text=element_text(size=10)
        )
    )

    graph_top_10_users_by_tag
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
        F.sum("nb_tweets").alias("somme_tweets"),
        F.count("date").alias("nb_journees"),

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

    ## Pour le tag du {dropdown.value} avec {stats["somme_tweets"]} tweets pour {stats["nb_journees"]} journées

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
