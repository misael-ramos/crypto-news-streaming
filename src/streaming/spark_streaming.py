"""
PySpark Structured Streaming — consome noticias do Kafka,
aplica analise de sentimento com VADER e salva resultados no S3.

Arquitetura:
  Kafka (crypto-news-raw)
      -> Spark Structured Streaming
          -> parse JSON
          -> VADER sentiment (UDF)
          -> classificacao BULLISH/BEARISH/NEUTRAL
          -> agregacao por janela de 5 min
      -> S3 (Parquet) + console output
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, window, count,
    avg, current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType
)

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import KAFKA_BROKER, KAFKA_TOPIC_RAW, AWS_BUCKET, AWS_REGION, S3_OUTPUT


# ============================================================
# Schema do JSON que vem do Kafka
# ============================================================
NEWS_SCHEMA = StructType([
    StructField("id",           StringType(), True),
    StructField("title",        StringType(), True),
    StructField("summary",      StringType(), True),
    StructField("link",         StringType(), True),
    StructField("source",       StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("collected_at", StringType(), True),
])


# ============================================================
# UDF de sentimento com VADER
# NOTA: UDF e necessaria aqui porque VADER e uma biblioteca
# Python pura — nao existe equivalente nativo no Spark.
# Para mitigar o custo da UDF, aplicamos ela uma unica vez
# e cache o resultado antes das agregacoes.
# ============================================================
def analyze_sentiment(text: str) -> str:
    """
    Classifica o sentimento de um texto usando VADER.
    VADER e otimizado para textos curtos de redes sociais e noticias.

    Retorna:
      BULLISH  — compound >= 0.05 (sentimento positivo)
      BEARISH  — compound <= -0.05 (sentimento negativo)
      NEUTRAL  — entre -0.05 e 0.05
    """
    if not text:
        return "NEUTRAL"
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
        scores   = analyzer.polarity_scores(text)
        compound = scores["compound"]

        if compound >= 0.05:
            return "BULLISH"
        elif compound <= -0.05:
            return "BEARISH"
        else:
            return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


def get_sentiment_score(text: str) -> float:
    """Retorna o score numerico de sentimento (-1 a +1)."""
    if not text:
        return 0.0
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
        return float(analyzer.polarity_scores(text)["compound"])
    except Exception:
        return 0.0


# Registra UDFs no Spark
sentiment_label_udf = udf(analyze_sentiment, StringType())
sentiment_score_udf = udf(get_sentiment_score, FloatType())


def create_spark_session(use_s3: bool = False) -> SparkSession:
    """
    Cria SparkSession com suporte a Kafka e opcionalmente S3.

    O pacote spark-sql-kafka e necessario para o Structured Streaming
    ler diretamente de topicos Kafka sem codigo boilerplate.
    """
    builder = SparkSession.builder \
        .appName("CryptoNewsSentimentStreaming") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")

    if use_s3:
        builder = builder \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    return builder.getOrCreate()


def run(use_s3: bool = False):
    """
    Pipeline de streaming principal.

    1. Le mensagens brutas do Kafka (bytes)
    2. Desserializa JSON com schema definido
    3. Aplica VADER para classificar sentimento
    4. Agrega por janela de 5 minutos
    5. Escreve resultados no console e opcionalmente no S3
    """
    spark = create_spark_session(use_s3)
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nConectando ao Kafka: {KAFKA_BROKER}")
    print(f"Consumindo topico: {KAFKA_TOPIC_RAW}\n")

    # ── 1. Leitura do Kafka ──────────────────────────────────
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC_RAW) \
        .option("startingOffsets", "latest") \
        .load()

    # ── 2. Desserializa o valor (bytes -> JSON) ──────────────
    df_parsed = df_raw \
        .select(from_json(col("value").cast("string"), NEWS_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("collected_at", to_timestamp("collected_at"))

    # ── 3. Analise de sentimento com VADER (UDF) ─────────────
    # Concatena titulo + sumario para analise mais rica
    from pyspark.sql.functions import concat_ws
    df_sentiment = df_parsed \
        .withColumn("text", concat_ws(" ", col("title"), col("summary"))) \
        .withColumn("sentiment",       sentiment_label_udf(col("text"))) \
        .withColumn("sentiment_score", sentiment_score_udf(col("text")))

    # ── 4. Agregacao por janela de 5 minutos ─────────────────
    # Conta quantas noticias BULLISH/BEARISH/NEUTRAL por janela
    df_agg = df_sentiment \
        .withWatermark("collected_at", "10 minutes") \
        .groupBy(
            window(col("collected_at"), "5 minutes"),
            col("sentiment")
        ) \
        .agg(
            count("*").alias("total_noticias"),
            avg("sentiment_score").alias("score_medio")
        )

    # ── 5. Output: console (debug) ───────────────────────────
    query_console = df_sentiment \
        .select("collected_at", "sentiment", "sentiment_score", "title", "source") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    # ── 6. Output: S3 ou local (Parquet) ────────────────────
    if use_s3:
        output_path     = f"s3a://{AWS_BUCKET}/{S3_OUTPUT}"
        checkpoint_path = f"s3a://{AWS_BUCKET}/checkpoints/sentiment"
    else:
        output_path     = "output/sentiment"
        checkpoint_path = "checkpoints/sentiment"

    query_sink = df_sentiment \
        .select(
            "id", "title", "source", "sentiment",
            "sentiment_score", "collected_at"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="60 seconds") \
        .start()

    print("Streaming iniciado. Aguardando mensagens do Kafka...\n")
    print("  Console output: a cada 30 segundos")
    print("  Parquet output: a cada 60 segundos")
    print("  Pressione Ctrl+C para parar.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStreaming encerrado.")
        query_console.stop()
        query_sink.stop()


if __name__ == "__main__":
    use_s3 = "--s3" in sys.argv
    run(use_s3=use_s3)
