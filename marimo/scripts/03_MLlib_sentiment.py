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
    # Machine learning : analyse de sentiment dans les tweets de la data

    Dans ce notebook, vous osberverez toute l'exécution pour l'analyse de sentiments, notamment :
    - la lecture du fichier d'entraînement et de test sur le site [kaggle](https://www.kaggle.com/datasets/krishbaisoya/tweets-sentiment-analysis?select=train_data.csv)
    - le nettoyage et transformation des données pour le modèle
    - création et entraînement du modèle
    - évaluation du modèle
    - application du modèle sur tous le jeu de données et sauvegarde des résultats dans SQL
    - visualisation graphique des résultats des prédictions
    """)
    return


@app.cell
def _():
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ArrayType)
    from utils.mastodon_utils import get_spark, read_table, write_table, JDBC_URL, JDBC_PROPS, DATA_DIR

    spark = get_spark("03-ml")
    return (
        F,
        IntegerType,
        StringType,
        StructField,
        StructType,
        read_table,
        spark,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.1 Récupérer le jeu de données brutes d'entraînement et de test

    Récupérons d'abord les jeu de données CSV avec comme valeur 0 négative et 1 pour positive
    """)
    return


@app.cell
def _(IntegerType, StringType, StructField, StructType):
    file_path = "/data"
    file_train_path = "/models/test/test_data.csv"
    file_test_path = "/models/train/train_data.csv"
    schema = StructType([
        StructField("content", StringType(), False),
        StructField('sentiment', IntegerType(), False)
    ])
    return file_path, file_test_path, file_train_path, schema


@app.cell
def _(file_path, file_test_path, file_train_path, schema, spark):
    df_tweets_train = spark.read.csv(file_path+file_train_path, sep=",", schema=schema, header=True)
    df_tweets_test = spark.read.csv(file_path+file_test_path, sep=",", schema=schema, header=True)
    return df_tweets_test, df_tweets_train


@app.cell
def _(df_tweets_test):
    df_tweets_test.show()
    return


@app.cell
def _(df_tweets_train):
    df_tweets_train.show()
    return


@app.cell
def _(df_tweets_test):
    # Précision des dimensions
    dim_lignes_test = df_tweets_test.count()
    dim_colonnes_test = len(df_tweets_test.columns)
    print("Nombre de lignes test : ", dim_lignes_test)
    print("Nombre de colonnes test : ", dim_colonnes_test)
    return (dim_lignes_test,)


@app.cell
def _(df_tweets_train):
    # Précision des dimensions
    dim_lignes_train = df_tweets_train.count()
    dim_colonnes_train = len(df_tweets_train.columns)
    print("Nombre de lignes train : ", dim_lignes_train)
    print("Nombre de colonnes train : ", dim_colonnes_train)
    return (dim_lignes_train,)


@app.cell
def _(dim_lignes_test, dim_lignes_train):
    somme = (dim_lignes_test+dim_lignes_train)
    proportion_train = dim_lignes_train / somme
    proportion_test = dim_lignes_test /somme 
    print("Proportion avec les deux datasets combinés : ", 
         round(proportion_test*100, 2),
          " % test / ",
         round(proportion_train*100, 2),
         " % train")
    return


@app.cell
def _(F, df_tweets_test, df_tweets_train):
    # Répartition selon le valeur cible
    print("Répartition pour l'entraînement : ")
    df_tweets_train.groupBy("sentiment").agg(F.count("content").alias("effectif")).show()
    print("---------------------------------\n")

    print("Répartition pour le test : ")
    df_tweets_test.groupBy("sentiment").agg(F.count("content").alias("effectif")).show()
    print("---------------------------------\n")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Commentaire :

    La répartition des valeurs cibles sont équilibrées ce qui est positives pour l'analyse de sentiment sans le risque de déséquilibrage.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 1.2 Nettoyage du jeu de données
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Commentaire :

    Ici, nous allons créer un DataFrame à utiliser pour observer les offres et les enlever dans l'exécution du DataFrame ci-dessous pour n'avoir qu'à faire à des tweets autre que des offres.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Enlever les espaces en trop et les éventuels symboles
    """)
    return


@app.cell
def _(F, df_tweets_test, df_tweets_train):
    df_tweets_train_cleaned = df_tweets_train.withColumn(
        "content_clean",
            F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.col("content"),
                    r"[^a-zA-ZÀ-ÿ0-9\s]",
                    ""
                ),
                r"\s+",
                " "
            )
            )
    )

    df_tweets_test_cleaned = df_tweets_test.withColumn(
        "content_clean",
            F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    F.col("content"),
                    r"[^a-zA-ZÀ-ÿ0-9\s]",
                    ""
                ),
                r"\s+",
                " "
            )
            )
    )
    return df_tweets_test_cleaned, df_tweets_train_cleaned


@app.cell
def _(df_tweets_train_cleaned):
    # Vérification du plan d'action
    df_tweets_train_cleaned.explain()
    return


@app.cell
def _(df_tweets_train_cleaned):
    df_tweets_train_cleaned.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Commentaire :

    Le nettoyage du texte et des tweets étant faite, on peut maintenant se concentrer sur l'analyse de ces textes en commançant par la tokenisation et la vectorisation du texte.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 1.3 Standardisation et vectorisation du texte
    """)
    return


@app.cell
def _():
    import pyspark.ml.feature as ml

    return (ml,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Tokenisation des tweets
    """)
    return


@app.cell
def _(ml):
    # Création d'un tokenizer pour séparer chaque mot d'un texte
    tokenizer = ml.Tokenizer(
        inputCol="content_clean",
        outputCol="words"
    )
    return (tokenizer,)


@app.cell
def _(df_tweets_test_cleaned, df_tweets_train_cleaned, tokenizer):
    # Application du tokenizer
    df_tweets_train_tokenized = tokenizer.transform(df_tweets_train_cleaned)
    df_tweets_test_tokenized = tokenizer.transform(df_tweets_test_cleaned)
    return df_tweets_test_tokenized, df_tweets_train_tokenized


@app.cell
def _(df_tweets_train_tokenized):
    df_tweets_train_tokenized.show()
    return


@app.cell
def _(df_tweets_test_tokenized):
    df_tweets_test_tokenized.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Suppression des stop words (english version)
    """)
    return


@app.cell
def _(df_tweets_test_tokenized, df_tweets_train_tokenized, ml):
    stopwords_remover = ml.StopWordsRemover(
        inputCol="words",
        outputCol="filtered_words",
        stopWords=ml.StopWordsRemover.loadDefaultStopWords("english")
    )

    df_tweets_train_stopwords = stopwords_remover.transform(df_tweets_train_tokenized)

    df_tweets_test_stopwords = stopwords_remover.transform(df_tweets_test_tokenized)
    return (
        df_tweets_test_stopwords,
        df_tweets_train_stopwords,
        stopwords_remover,
    )


@app.cell
def _(df_tweets_train_stopwords):
    # Vérification du succès de l'opération
    df_tweets_train_stopwords.show()
    return


@app.cell
def _(df_tweets_test_stopwords):
    # Vérification du succès de l'opération
    df_tweets_test_stopwords.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Vectorisation des mots et application de l'IDF
    """)
    return


@app.cell
def _(ml):
    # Initialisation de la vectorisation
    count_vectorizer = ml.CountVectorizer(
        inputCol="filtered_words",
        outputCol="raw_features",
        vocabSize=50000,
        minDF=2.0
    )

    # Initialisation de l'IDF
    idf = ml.IDF(
        inputCol="raw_features",
        outputCol="features"
    )
    return count_vectorizer, idf


@app.cell
def _(count_vectorizer, df_tweets_train_stopwords):
    cv_model = count_vectorizer.fit(df_tweets_train_stopwords)

    df_vectorized_train = cv_model.transform(df_tweets_train_stopwords)

    return (df_vectorized_train,)


@app.cell
def _(df_vectorized_train):
    # Vérification du succès de l'opération pour la vectorisation
    df_vectorized_train.show()
    return


@app.cell
def _(df_vectorized_train, idf):
    # Application de l'IDF apportant un poids au mot et à leur distance
    idf_model = idf.fit(df_vectorized_train)

    df_vectorized_train_idf = idf_model.transform(df_vectorized_train)
    return (df_vectorized_train_idf,)


@app.cell
def _(df_vectorized_train_idf):
    df_vectorized_train_idf.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 1.4 Définition du modèle, entraînement avec évaluation
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Type de modèle d'analyse de sentiment
    """)
    return


@app.cell
def _():
    from pyspark.ml.classification import LogisticRegression # Régression logistique
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator # Pour l'évaluation du modèle

    return (
        BinaryClassificationEvaluator,
        LogisticRegression,
        MulticlassClassificationEvaluator,
    )


@app.cell
def _(LogisticRegression):
    # Paramérage de la régression logistique
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="sentiment",
        maxIter=50,
        regParam=0.01
    )
    return (lr,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Commentaire :

    Pour chaque étape de préparation avant entraînement du modèle, nous avons montré les résultats pour voir ce qu'il donnait et comprendre comment son calculés ces vecteurs pour notre analyse de sentiment.

    Afin d'éviter de répéter les mêmes étapes de pré-traitement, il est préférable d'utiliser une pipeline qui exécute tous ces traitements en donnant juste le DataFrame directement train.
    """)
    return


@app.cell
def _():
    from pyspark.ml import Pipeline # Pour construire la pipeline

    return (Pipeline,)


@app.cell
def _(Pipeline, count_vectorizer, idf, lr, stopwords_remover, tokenizer):
    # Création de la pipeline de transformation jusqu'à l'application du modèle
    pipeline = Pipeline(
        stages=[
            tokenizer, # Tokenisation
            stopwords_remover, # Enlèvement des stopwords anglais
            count_vectorizer, # Vectorisation
            idf, # Calcul distance IDF
            lr # Application du modèle de régression logistique
        ]
    )
    return (pipeline,)


@app.cell
def _(df_tweets_train_cleaned, pipeline):
    # Entraînement du modèle via le dataset de training
    model = pipeline.fit(df_tweets_train_cleaned)
    return (model,)


@app.cell
def _(df_tweets_test_cleaned, model):
    # Prédictions
    predictions = model.transform(df_tweets_test_cleaned)
    return (predictions,)


@app.cell
def _(MulticlassClassificationEvaluator):
    # Évaluation
    evaluator = MulticlassClassificationEvaluator(
        labelCol="sentiment",
        predictionCol="prediction",
        metricName="accuracy"
    )
    return (evaluator,)


@app.cell
def _(evaluator, predictions):
    accuracy = evaluator.evaluate(predictions)

    print(f"Accuracy : {accuracy:.4f}")
    return (accuracy,)


@app.cell(hide_code=True)
def _(accuracy, mo):
    mo.md(f"""
    ### Commentaire : 

    Le modèle prédit le bon sentiment d'un tweet à {accuracy*100:.2f} %
    """)
    return


@app.cell
def _(MulticlassClassificationEvaluator, predictions):
    # Calcul du F1-score
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="sentiment",
        predictionCol="prediction",
        metricName="f1"
    )

    f1 = f1_evaluator.evaluate(predictions)
    return (f1,)


@app.cell
def _(f1):
    print(f1)
    return


@app.cell
def _(BinaryClassificationEvaluator, predictions):
    # 3. Aire sous la courbe ROC (AUC-ROC)
    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="sentiment",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    auc = auc_evaluator.evaluate(predictions)
    return (auc,)


@app.cell
def _(auc):
    print(auc)
    return


@app.cell(hide_code=True)
def _(accuracy, auc, f1, mo):
    mo.md(f"""
    ## Évaluation du modèle de sentiment

    Le modèle de classification a été évalué à l'aide de trois métriques :
    l'**Accuracy**, le **F1-score** et l'**AUC-ROC**. Elles permettent de précisément savoir si le modèle est performant dans la classification de sentiment d'un tweet.

    ### Résultats obtenus

    | Métrique | Résultat |
    |:--|--:|
    | **Accuracy** | {accuracy:.2%} |
    | **F1-score** | {f1:.4f} |
    | **AUC-ROC** | {auc:.4f} | 

    ### Conclusion

    Les résultats montrent que le modèle parvient à identifier une partie des
    sentiments, mais que ses performances restent perfectibles. Nous allons observer les prédictions mal faites via une matrice de confusion
    """)
    return


@app.cell
def _():
    from pyspark.sql.window import Window

    return (Window,)


@app.cell
def _(F, predictions):
    # Calculer la matrice de confusion
    confusion_spark = (
        predictions
        .filter(
            F.col("sentiment").isNotNull()
            & F.col("prediction").isNotNull()
        )
        .groupBy("sentiment", "prediction")
        .agg(F.count("*").alias("effectif"))
    )

    confusion_spark.show()
    return (confusion_spark,)


@app.cell
def _(F, Window, confusion_spark):
    # Calculer le total de chaque classe réelle
    window_label = Window.partitionBy("sentiment")

    # Normaliser par ligne : chaque classe réelle totalise 100 %
    confusion_spark_prct = (
        confusion_spark
        .withColumn(
            "total_reel",
            F.sum("effectif").over(window_label)
        )
        .withColumn(
            "pourcentage",
            F.round(
                100 * F.col("effectif") / F.col("total_reel"),
                2
            )
        )
    )
    return (confusion_spark_prct,)


@app.cell
def _(confusion_spark_prct):
    confusion_spark_prct.orderBy("sentiment", "prediction").show()
    return


@app.cell
def _(confusion_spark_prct):
    import pandas as pd

    confusion_pd = confusion_spark_prct.toPandas()

    # Libellés des classes
    labels_sentiment = {
        0.0: "Négatif",
        1.0: "Positif",
        0: "Négatif",
        1: "Positif"
    }

    confusion_pd["Réel"] = confusion_pd["sentiment"].map(labels_sentiment)
    confusion_pd["Prédit"] = confusion_pd["prediction"].map(labels_sentiment)
    return confusion_pd, pd


@app.cell
def _(confusion_pd, pd):
    # Visualisation graphique de la matrice de confusion
    from plotnine import (
        ggplot, aes, geom_tile, geom_text,
        labs, theme_minimal, theme,
        element_text, scale_fill_gradient
    )

    # Ordonner les catégories pour faciliter la lecture
    confusion_pd["Réel"] = pd.Categorical(
        confusion_pd["Réel"],
        categories=["Positif", "Négatif"],
        ordered=True
    )

    confusion_pd["Prédit"] = pd.Categorical(
        confusion_pd["Prédit"],
        categories=["Négatif", "Positif"],
        ordered=True
    )

    matrice_confusion = (
        ggplot(
            confusion_pd,
            aes(x="Prédit", y="Réel", fill="pourcentage")
        )
        + geom_tile(color="white", size=1)
        + geom_text(
            aes(label="pourcentage"),
            format_string="{:.1f} %",
            size=12,
            color="black"
        )
        + scale_fill_gradient(
            low="#F7FBFF",
            high="#2171B5",
            name="Pourcentage"
        )
        + labs(
            title="Matrice de confusion normalisée",
            subtitle="Pourcentage des prédictions au sein de chaque classe réelle",
            x="Sentiment prédit",
            y="Sentiment réel"
        )
        + theme_minimal()
        + theme(
            figure_size=(8, 6),
            plot_title=element_text(size=15, weight="bold"),
            plot_subtitle=element_text(size=10),
            axis_title=element_text(size=12),
            axis_text=element_text(size=11)
        )
    )

    matrice_confusion
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 1.5 Sauvegarde du modèle résultant
    """)
    return


@app.cell
def _():
    import os
    MODEL_PATH="/data/models/result/"
    return MODEL_PATH, os


@app.cell
def _(MODEL_PATH, os):
    # Sauvegarder le pipeline entrainé
    MODEL_PATH_FILE = os.path.join(MODEL_PATH, "sentiment_lr")
    return (MODEL_PATH_FILE,)


@app.cell
def _(MODEL_PATH_FILE, model):
    # Sauvegarder le pipeline entraîné
    model.write().overwrite().save(MODEL_PATH_FILE)
    print(f"Modèle sauvegardé dans : {MODEL_PATH_FILE}")
    return


@app.cell
def _():
    # Recharger le modèle entrainé avec la méthode PipelineModel
    from pyspark.ml import PipelineModel

    return (PipelineModel,)


@app.cell
def _(MODEL_PATH_FILE, PipelineModel):
    sentiment_model = PipelineModel.load(MODEL_PATH_FILE)

    print("Modèle de sentiment rechargé avec succès.")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 1.6 Application sur les tweets de la data

    Maintenant que nous avons créé notre modèle d'analyse de sentiments, nous allons l'appliquer sur les tweets de la data pour observer ce qu'il en ressort comme sentiment.

    Nous allons d'abord recupérer ces données, appliquer un nettoyage sur les symboles à enlever et les espaces en trop. Ensuite, après application du modèle, enregistrer le résultats dans une table SQL à part avec la conservation du tag utilisé pour capturer ce tweet, son contenu et le sentiment de ce contenu.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Charger les tweets de la data
    """)
    return


@app.cell
def _(read_table, spark):
    read_table(spark, "")
    return


if __name__ == "__main__":
    app.run()
