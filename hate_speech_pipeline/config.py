<<<<<<< Updated upstream
=======
# import os
# import yaml

# class Config:
#     def __init__(self, config_path: str = "d:/hate_speech/sense_media/Reddit-Hate-speech-Analysis/config.yml"):
#         with open(config_path, 'r') as f:
#             self.cfg = yaml.safe_load(f)
#         self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
#         self.kafka_topic = self.cfg.get("Kafka", {}).get("reddit_topic", "reddit_topic")
#         self.es_host = os.getenv("ES_HOST", "http://localhost:9200")
#         self.es_index = os.getenv("ES_INDEX", "news_index")
#         self.news_api_key = os.getenv("NEWS_API_KEY")

#     def get(self, section, key, default=None):
#         return self.cfg.get(section, {}).get(key, default)


# import os
# import yaml

# class Config:
#     def __init__(self, config_path: str = None):
#         if config_path is None:
#             # Automatically find config.yml in the project root
#             base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#             config_path = os.path.join(base_dir, "config.yml")

#         with open(config_path, 'r') as f:
#             self.cfg = yaml.safe_load(f)

#         self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
#         self.kafka_topic = self.cfg.get("Kafka", {}).get("reddit_topic", "reddit_topic")
#         self.es_host = os.getenv("ES_HOST", "http://localhost:9200")
#         self.es_index = os.getenv("ES_INDEX", "news_index")
#         self.news_api_key = os.getenv("NEWS_API_KEY")

#     def get(self, section, key, default=None):
#         return self.cfg.get(section, {}).get(key, default)


>>>>>>> Stashed changes
import os
import yaml

class Config:
    def __init__(self, config_path: str = "d:/hate_speech/sense_media/Reddit-Hate-speech-Analysis/config.yml"):
        with open(config_path, 'r') as f:
            self.cfg = yaml.safe_load(f)
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.kafka_topic = self.cfg.get("Kafka", {}).get("reddit_topic", "reddit_topic")
        self.es_host = os.getenv("ES_HOST", "http://localhost:9200")
        self.es_index = os.getenv("ES_INDEX", self.cfg.get("Elasticsearch", {}).get("index", "reddit_hate"))  # ✅ FIXED
        self.news_api_key = os.getenv("NEWS_API_KEY")

    def get(self, section, key, default=None):
        return self.cfg.get(section, {}).get(key, default)
