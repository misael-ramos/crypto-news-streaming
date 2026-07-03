"""
Producer Kafka — coleta noticias de RSS feeds e publica no topico crypto-news-raw.

Fluxo:
  1. Faz polling dos RSS feeds a cada FETCH_INTERVAL segundos
  2. Filtra noticias que contem palavras-chave de cripto
  3. Serializa como JSON e publica no topico Kafka
  4. Controla duplicatas via set de IDs ja publicados
"""

import json
import time
import hashlib
import feedparser
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import (
    KAFKA_BROKER, KAFKA_TOPIC_RAW,
    RSS_FEEDS, CRYPTO_KEYWORDS, FETCH_INTERVAL
)


def create_producer() -> KafkaProducer:
    """
    Cria o KafkaProducer com serialização JSON.
    value_serializer converte dict Python para bytes JSON automaticamente.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",           # aguarda confirmação de todos os replicas
        retries=3,            # tenta 3 vezes em caso de falha
        max_block_ms=5000,    # timeout de 5s para conectar
    )


def is_crypto_relevant(title: str, summary: str) -> bool:
    """
    Verifica se a noticia e relevante para o universo cripto.
    Checa titulo e sumario contra lista de palavras-chave.
    """
    text = (title + " " + summary).lower()
    return any(kw in text for kw in CRYPTO_KEYWORDS)


def generate_id(title: str, link: str) -> str:
    """
    Gera um ID unico para cada noticia baseado no titulo + link.
    Usado para evitar publicar a mesma noticia duas vezes.
    """
    raw = (title + link).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def fetch_feed(url: str) -> list[dict]:
    """
    Faz o parse de um RSS feed e retorna lista de noticias.
    feedparser e tolerante a feeds malformados — nao levanta excecoes.
    """
    try:
        feed = feedparser.parse(url)
        return feed.entries
    except Exception as e:
        print(f"  Erro ao ler feed {url}: {e}")
        return []


def run():
    """
    Loop principal do producer.
    A cada FETCH_INTERVAL segundos:
      1. Faz polling de todos os RSS feeds
      2. Filtra noticias relevantes e novas
      3. Publica no topico Kafka
    """
    print(f"Conectando ao Kafka em {KAFKA_BROKER}...")
    try:
        producer = create_producer()
        print(f"Conectado. Publicando em topico: {KAFKA_TOPIC_RAW}\n")
    except NoBrokersAvailable:
        print("Erro: Kafka nao disponivel. Verifique se o Docker esta rodando.")
        return

    # controle de duplicatas em memoria
    published_ids: set = set()

    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fazendo polling dos feeds...")
        total_published = 0

        for feed_url in RSS_FEEDS:
            entries = fetch_feed(feed_url)

            for entry in entries:
                title   = entry.get("title", "")
                summary = entry.get("summary", "")
                link    = entry.get("link", "")
                pub_date = entry.get("published", "")

                # filtra por relevancia
                if not is_crypto_relevant(title, summary):
                    continue

                # filtra duplicatas
                news_id = generate_id(title, link)
                if news_id in published_ids:
                    continue

                # monta o payload
                payload = {
                    "id":          news_id,
                    "title":       title,
                    "summary":     summary[:500],   # limita tamanho
                    "link":        link,
                    "source":      feed_url,
                    "published_at": pub_date,
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                }

                # publica no Kafka
                producer.send(KAFKA_TOPIC_RAW, value=payload)
                published_ids.add(news_id)
                total_published += 1

                print(f"  ✅ [{title[:60]}...]")

        producer.flush()
        print(f"  Total publicado neste ciclo: {total_published} noticias")
        print(f"  Proximo polling em {FETCH_INTERVAL}s...")
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    run()
