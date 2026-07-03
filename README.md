# 📡 crypto-news-streaming

Pipeline de **streaming de dados em tempo real** que coleta notícias financeiras de cripto, aplica análise de sentimento com VADER e processa com **Apache Kafka + PySpark Structured Streaming**.

## 🎯 Objetivo de negócio

Detectar o sentimento do mercado de criptomoedas em tempo real baseado em notícias — antecipando movimentos de preço antes do mercado reagir.

| Sentimento | Classificação | Sinal |
|---|---|---|
| compound ≥ 0.05 | BULLISH 📈 | Notícia positiva para o mercado |
| compound ≤ -0.05 | BEARISH 📉 | Notícia negativa para o mercado |
| entre -0.05 e 0.05 | NEUTRAL ➡️ | Sem viés claro |

## 🏗️ Arquitetura

```
RSS Feeds (CoinDesk, Cointelegraph, Decrypt, Bitcoin Magazine)
        ↓
Python Producer (feedparser + kafka-python)
        ↓
Kafka Topic: crypto-news-raw
        ↓
PySpark Structured Streaming
  ├── Desserialização JSON com schema definido
  ├── Análise de sentimento VADER (UDF)
  ├── Classificação: BULLISH / BEARISH / NEUTRAL
  └── Agregação por janela de 5 minutos (watermark 10 min)
        ↓
Parquet local / S3 + Console output
```

## 🛠️ Stack

- **Apache Kafka** (via Docker) — message broker
- **PySpark Structured Streaming** — processamento em tempo real
- **VADER** — análise de sentimento otimizada para textos curtos
- **feedparser** — coleta de RSS feeds
- **Amazon S3** — sink dos resultados (opcional)

## 🚀 Como executar

### 1. Pré-requisitos
```bash
# Docker instalado e rodando
docker --version

# Subir Kafka
docker compose up -d
```

### 2. Ambiente Python
```bash
python3 -m venv venv && source venv/bin/activate
pip3 install -r requirements.txt
```

### 3. Rodar o Producer (Terminal 1)
```bash
# coleta noticias dos RSS feeds e publica no Kafka
python3 -m src.producer.news_producer
```

### 4. Rodar o Streaming (Terminal 2)
```bash
# consome do Kafka, aplica VADER e salva Parquet
python3 -m src.streaming.spark_streaming

# com S3
python3 -m src.streaming.spark_streaming --s3
```

## 📁 Estrutura

```
crypto-news-streaming/
├── docker-compose.yml       # Kafka + Zookeeper
├── config/
│   └── settings.py          # feeds, topics, AWS
├── src/
│   ├── producer/
│   │   └── news_producer.py # coleta RSS → Kafka
│   └── streaming/
│       └── spark_streaming.py # Kafka → VADER → Parquet
├── requirements.txt
└── README.md
```

## 💡 Decisões técnicas

| Decisão | Justificativa |
|---|---|
| RSS em vez de Twitter API | API do Twitter/X é paga ($100/mês). RSS feeds são gratuitos, atualizados em tempo real e sem autenticação |
| VADER em vez de BERT | VADER é leve, rápido e otimizado para textos curtos de notícias. BERT exigiria GPU para ser viável em streaming |
| UDF para VADER | VADER é Python puro — não existe equivalente nativo no Spark. UDF é necessária e aplicada uma única vez antes das agregações |
| Watermark de 10 min | Garante que mensagens atrasadas até 10 minutos ainda sejam processadas corretamente |
| Trigger de 60s no sink | Equilibrio entre latência e custo de I/O no S3 |

## 🔗 Projetos relacionados

- [crypto-pipeline](https://github.com/misael-ramos/crypto-pipeline) — ETL batch com S3 e Athena
- [crypto-dw](https://github.com/misael-ramos/crypto-dw) — Data Warehouse com Star Schema
- [crypto-seasonality-spark](https://github.com/misael-ramos/crypto-seasonality-spark) — análise histórica com PySpark
