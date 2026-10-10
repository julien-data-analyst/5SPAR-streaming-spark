import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Data Visualization — Analyse des publications Mastodon

    Cette partie présente les résultats des traitements de données sous forme de graphiques réalisés avec Seaborn.
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    # Analyse de l'activité des utilisateurs Mastodon

    **Projet Data Engineering — PySpark et Seaborn**

    Ce notebook permet d'analyser l'activité des utilisateurs
    Mastodon et de visualiser les résultats avec Seaborn.
    """)
    return


@app.cell
def _():
    from pyspark.sql import functions as F
    import seaborn as sns
    import matplotlib.pyplot as plt

    from utils.mastodon_utils import get_spark, read_table

    spark = get_spark(
        "batch-mastodon-user-activity",
        shuffle_partitions=4
    )
    return F, plt, read_table, sns, spark


@app.cell
def _(read_table, spark):
    user_activity = (
        read_table(spark, "mastodon_stream")
        .dropDuplicates(["tweet_id"])
    )

    user_activity.show(5)
    return (user_activity,)


@app.cell
def _(F, user_activity):
    top_10_users = (
        user_activity
        .groupBy("account_id", "account_username")
        .count()
        .orderBy(F.col("count").desc())
        .limit(10)
    )

    top_10_users.show()
    return (top_10_users,)


@app.cell
def _(plt, sns, top_10_users):
    top_10_pandas = top_10_users.toPandas()

    fig, ax = plt.subplots(figsize=(10, 6))

    sns.barplot(
        data=top_10_pandas,
        x="count",
        y="account_username",
        color="steelblue",
        ax=ax
    )

    ax.set_title("Top 10 des utilisateurs les plus actifs sur Mastodon")
    ax.set_xlabel("Nombre de publications")
    ax.set_ylabel("Utilisateurs")

    plt.tight_layout()

    fig
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Analyse

    Le graphique montre que **hackaday, spiritul et feed** sont les utilisateurs les plus actifs. L'activité est donc concentrée sur quelques comptes qui publient davantage que les autres.
    """)
    return


@app.cell
def _(user_activity):
    publications_par_utilisateur = (
        user_activity
        .groupBy("account_id")
        .count()
        .withColumnRenamed("count", "nb_publications")
    )

    publications_par_utilisateur.show(10)
    return (publications_par_utilisateur,)


@app.cell
def _(F, publications_par_utilisateur):
    repartition_activite = (
        publications_par_utilisateur
        .withColumn(
            "niveau_activite",
            F.when(F.col("nb_publications") == 1, "1 publication")
             .when(F.col("nb_publications") <= 5, "2 à 5 publications")
             .when(F.col("nb_publications") <= 20, "6 à 20 publications")
             .otherwise("Plus de 20 publications")
        )
        .groupBy("niveau_activite")
        .count()
        .withColumnRenamed("count", "nb_utilisateurs")
    )

    repartition_activite.show()
    return (repartition_activite,)


@app.cell
def _(plt, repartition_activite, sns):
    repartition_pandas = repartition_activite.toPandas()

    ordre = [
        "1 publication",
        "2 à 5 publications",
        "6 à 20 publications",
        "Plus de 20 publications"
    ]

    fig2, ax2 = plt.subplots(figsize=(10, 5))

    sns.barplot(
        data=repartition_pandas,
        x="niveau_activite",
        y="nb_utilisateurs",
        order=ordre,
        color="steelblue",
        ax=ax2
    )

    ax2.set_title("Répartition de l'activité des utilisateurs Mastodon")
    ax2.set_xlabel("Niveau d'activité")
    ax2.set_ylabel("Nombre d'utilisateurs")

    plt.tight_layout()

    fig2
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Analyse

    La majorité des utilisateurs publient peu de messages : **252 utilisateurs n'ont publié qu'une seule fois**, contre seulement **21 utilisateurs ayant publié plus de 20 fois**. L'activité est donc concentrée sur un petit groupe d'utilisateurs très actifs.
    """)
    return


@app.cell
def _(F, user_activity):
    longueur_moyenne_users = (
        user_activity
        .filter(F.col("content_clean").isNotNull())
        .withColumn(
            "longueur_message",
            F.length(F.col("content_clean"))
        )
        .groupBy("account_id", "account_username")
        .agg(
            F.round(F.avg("longueur_message"), 2).alias("longueur_moyenne"),
            F.count("*").alias("nb_publications")
        )
    )

    longueur_moyenne_users.show(10)
    return (longueur_moyenne_users,)


@app.cell
def _(F, longueur_moyenne_users, top_10_users):
    comparaison_users = (
        top_10_users
        .join(
            longueur_moyenne_users,
            on=["account_id", "account_username"],
            how="inner"
        )
        .select(
            "account_username",
            "longueur_moyenne",
            "nb_publications"
        )
        .orderBy(F.col("nb_publications").desc())
    )

    comparaison_users.show()
    return (comparaison_users,)


@app.cell
def _(comparaison_users, plt, sns):
    comparaison_pandas = comparaison_users.toPandas()

    fig3, ax3 = plt.subplots(figsize=(10, 6))

    sns.barplot(
        data=comparaison_pandas,
        x="longueur_moyenne",
        y="account_username",
        color="steelblue",
        ax=ax3
    )

    ax3.set_title("Longueur moyenne des publications des 10 utilisateurs les plus actifs")
    ax3.set_xlabel("Longueur moyenne (caractères)")
    ax3.set_ylabel("Utilisateurs")

    plt.tight_layout()

    fig3
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Analyse

    Le graphique montre que **hackaday** publie en moyenne des messages nettement plus longs que les autres utilisateurs du Top 10. Cela montre que les comptes les plus actifs n'ont pas tous les mêmes habitudes de publication.
    """)
    return


if __name__ == "__main__":
    app.run()
