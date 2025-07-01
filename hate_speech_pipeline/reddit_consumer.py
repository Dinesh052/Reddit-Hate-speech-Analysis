# import json
# import time
# from kafka import KafkaConsumer
# from elasticsearch import Elasticsearch
# from hate_speech_pipeline.model import HateSpeechModel
# from hate_speech_pipeline.config import Config


# def main():
#     config = Config()
#     consumer = KafkaConsumer(
#         config.kafka_topic,
#         bootstrap_servers=config.kafka_broker,
#         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#         auto_offset_reset='earliest',
#         enable_auto_commit=True
#     )
#     es = Elasticsearch(config.es_host)
#     model = HateSpeechModel()
#     index = "reddit_hate"
#     print("[RedditConsumer] Listening to Kafka topic and processing Reddit posts...")
#     for msg in consumer:
#         news = msg.value["message"]
#         # Only process Reddit posts (they have 'subreddit' key)
#         if 'subreddit' not in news:
#             continue
#         print(f"[RedditConsumer] Received from Kafka: {news['title']}")
#         desc = news.get("description", "")
#         hate = model.hate_speech(desc)
#         sentiment = model.sentiment(desc)
#         doc = {
#             **news,
#             "hate_label": hate.get("label"),
#             "hate_score": hate.get("score"),
#             "sentiment_label": sentiment.get("label"),
#             "sentiment_score": sentiment.get("score")
#         }
#         print(f"[RedditConsumer] Processed: {doc['title']} | Hate: {doc['hate_label']} | Sentiment: {doc['sentiment_label']}")
#         res = es.index(index=index, body=doc)
#         print(f"[RedditConsumer] Indexed to Elasticsearch: {doc['title']} (ID: {res['_id']})")
#         print(f"[RedditConsumer] Data now visible in Kibana (index: {index})!")
#         time.sleep(10)  # Wait 10 seconds before processing the next message for demonstration

# if __name__ == "__main__":
#     main()


import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from hate_speech_pipeline.model import HateSpeechModel
from hate_speech_pipeline.config import Config

def main():
    config = Config()
    consumer = KafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_broker,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    es = Elasticsearch(config.es_host)
    model = HateSpeechModel()
    index = "reddit_hate"
    print("[RedditConsumer] Listening to Kafka topic and processing Reddit posts...")

    for msg in consumer:
        news = msg.value["message"]
        if 'subreddit' not in news:
            continue

        desc = news.get("description", "")
        hate = model.hate_speech(desc)
        sentiment = model.sentiment(desc)

        print("\n[DEBUG] Input Text:", desc)
        print("[DEBUG] Hate Prediction:", hate)
        print("[DEBUG] Sentiment Prediction:", sentiment)

        doc = {
            **news,
            "hate_label": hate.get("label"),
            "hate_score": hate.get("score"),
            "sentiment_label": sentiment.get("label"),
            "sentiment_score": sentiment.get("score")
        }

        print("[DEBUG] Final Document to ES:\n", json.dumps(doc, indent=2))

        res = es.index(index=index, body=doc)
        print(f"[RedditConsumer] Indexed to Elasticsearch: {doc['title']} (ID: {res['_id']})")
        print(f"[RedditConsumer] Data now visible in Kibana (index: {index})!")
        time.sleep(10)

if __name__ == "__main__":
    main()
