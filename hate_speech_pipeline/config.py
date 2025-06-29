import os
import yaml

class Config:
    def __init__(self, config_path: str = "D:\hate_speech\sense_media\config.yml"):
        with open(config_path, 'r') as f:
            self.cfg = yaml.safe_load(f)
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.kafka_topic = self.cfg.get("NewsAPI", {}).get("topic", "Apple")
        self.es_host = os.getenv("ES_HOST", "http://localhost:9200")
        self.es_index = os.getenv("ES_INDEX", "news_index")
        self.news_api_key = os.getenv("NEWS_API_KEY")

    def get(self, section, key, default=None):
        return self.cfg.get(section, {}).get(key, default)
