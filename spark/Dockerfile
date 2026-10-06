FROM apache/spark:4.1.1-python3

USER root

# Environnement pour le build des packages
ENV HOME=/tmp
ENV IVY_HOME=/tmp/.ivy2

RUN mkdir -p /tmp/.ivy2 /opt/spark/jars-extra

RUN cat > /tmp/test_spark.py <<'EOF'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestPackages")
    .getOrCreate()
)

print("Spark version:", spark.version)

spark.stop()
EOF

# Téléchargement des librairies postgresql et kafka
RUN /opt/spark/bin/spark-submit   --conf spark.jars.ivy=/tmp/.ivy2   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.4   /tmp/test_spark.py

RUN echo "=== COPY JARS ===" && \
    find /tmp/.ivy2 -type f -name "*.jar" \
        -exec cp {} /opt/spark/jars/ \;

RUN echo "=== SPARK JARS ===" && \
    ls -lah /opt/spark/jars | grep -E "kafka|postgres"


RUN python3 -m pip install --no-cache-dir \
    "pandas>=2.2.0" \
    "pyarrow>=15.0.0" \
    "grpcio>=1.48.1" \
    "grpcio-status >= 1.48.1" \
    "zstandard >= 0.25.0"

#USER 185 # Bug pour la création des checkpoints !
