import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BROKER          = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_RAW       = os.getenv("KAFKA_TOPIC_RAW", "crypto-news-raw")
KAFKA_TOPIC_SENTIMENT = os.getenv("KAFKA_TOPIC_SENTIMENT", "crypto-news-sentiment")

# AWS
AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_OUTPUT  = "streaming/sentiment"

# RSS Feeds de noticias financeiras de cripto
RSS_FEEDS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://bitcoinmagazine.com/feed",
]

# Palavras-chave para filtrar noticias relevantes
CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto", "blockchain",
    "defi", "nft", "altcoin", "solana", "xrp", "binance",
    "market", "price", "rally", "crash", "bull", "bear",
]

# Intervalo de coleta em segundos
FETCH_INTERVAL = 60
