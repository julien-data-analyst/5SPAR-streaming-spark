import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Chargement des librairies
    """)
    return


@app.cell
def _():
    # Chargement des librairies
    import marimo as mo
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    return SparkSession, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Etape 1 : initialisation spark
    """)
    return


@app.cell
def _(SparkSession):
    # Création de la session Spark
    spark = SparkSession.builder.appName("ventes").getOrCreate()
    return


@app.cell
def _():
    return


@app.cell
def _():
    # Arrêter la session Spark
    #spark.stop()
    return


if __name__ == "__main__":
    app.run()
