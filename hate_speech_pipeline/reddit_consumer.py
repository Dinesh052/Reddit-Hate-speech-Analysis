<<<<<<< Updated upstream
=======
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
#         if 'subreddit' not in news:
#             continue

#         desc = news.get("description", "")
#         hate = model.hate_speech(desc)
#         sentiment = model.sentiment(desc)

#         print("\n[DEBUG] Input Text:", desc)
#         print("[DEBUG] Hate Prediction:", hate)
#         print("[DEBUG] Sentiment Prediction:", sentiment)

#         doc = {
#             **news,
#             "hate_label": hate.get("label"),
#             "hate_score": hate.get("score"),
#             "sentiment_label": sentiment.get("label"),
#             "sentiment_score": sentiment.get("score")
#         }
#         print("[DEBUG] Final hate_label going to ES:", doc["hate_label"])
#         print(model.hate_speech("Kill all people of that race."))

#         print("[DEBUG] Final Document to ES:\n", json.dumps(doc, indent=2))

#         res = es.index(index=index, body=doc)
#         print(f"[RedditConsumer] Indexed to Elasticsearch: {doc['title']} (ID: {res['_id']})")
#         print(f"[RedditConsumer] Data now visible in Kibana (index: {index})!")
#         time.sleep(10)

# if __name__ == "__main__":
#     main()


# import json
# import time
# from kafka import KafkaConsumer
# from elasticsearch import Elasticsearch
# from hate_speech_pipeline.model import HateSpeechModel
# from hate_speech_pipeline.config import Config

# def main():
#     config = Config()
    
#     # Connect to Kafka
#     consumer = KafkaConsumer(
#         config.kafka_topic,
#         bootstrap_servers=config.kafka_broker,
#         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#         auto_offset_reset='earliest',
#         enable_auto_commit=True
#     )
    
#     # Connect to Elasticsearch
#     es = Elasticsearch(config.es_host)

#     # Load model
#     model = HateSpeechModel()

#     print("[RedditConsumer] Listening to Kafka topic and processing Reddit posts...")

#     for message in consumer:
#         msg = message.value
#         post = msg.get("message")
#         if not post or 'subreddit' not in post:
#             continue

#         # Combine title and description for model input
#         text = f"{post.get('title', '')} {post.get('description', '')}".strip()

#         hate = model.hate_speech(text)
#         sentiment = model.sentiment(text)

#         # Debug output
#         print("\n[DEBUG] Input Text:", text)
#         print("[DEBUG] Hate Prediction:", hate)
#         print("[DEBUG] Sentiment Prediction:", sentiment)

#         # Add classification results to document
#         enriched_post = {
#             **post,
#             "hate_label": hate.get("label"),
#             "hate_score": hate.get("score"),
#             "sentiment_label": sentiment.get("label"),
#             "sentiment_score": sentiment.get("score")
#         }

#         # Index enriched document to Elasticsearch
#         try:
#             res = es.index(index=config.elasticsearch_index, document=enriched_post)
#             print(f"[RedditConsumer] Indexed to Elasticsearch (ID: {res['_id']})")
#         except Exception as e:
#             print(f"[ERROR] Failed to index document: {e}")

#         # Optional pause for readability
#         time.sleep(1)

# if __name__ == "__main__":
#     main()

>>>>>>> Stashed changes
import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from hate_speech_pipeline.model import HateSpeechModel
from hate_speech_pipeline.config import Config


def main():
    config = Config()
    
    # Connect to Kafka
    consumer = KafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_broker,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    # Connect to Elasticsearch
    es = Elasticsearch(config.es_host)

    # Load model
    model = HateSpeechModel()

    print("[RedditConsumer] Listening to Kafka topic and processing Reddit posts...")
<<<<<<< Updated upstream
    for msg in consumer:
        news = msg.value["message"]
        # Only process Reddit posts (they have 'subreddit' key)
        if 'subreddit' not in news:
            continue
        print(f"[RedditConsumer] Received from Kafka: {news['title']}")
        desc = news.get("description", "")
        hate = model.hate_speech(desc)
        sentiment = model.sentiment(desc)
        doc = {
            **news,
=======

    for message in consumer:
        msg = message.value
        post = msg.get("message")
        if not post or 'subreddit' not in post:
            continue

        # Combine title and description for model input
        text = f"{post.get('title', '')} {post.get('description', '')}".strip()

        hate = model.hate_speech(text)
        sentiment = model.sentiment(text)

        # Debug output
        print("\n[DEBUG] Input Text:", text)
        print("[DEBUG] Hate Prediction:", hate)
        print("[DEBUG] Sentiment Prediction:", sentiment)

        # Add classification results to document
        enriched_post = {
            **post,
>>>>>>> Stashed changes
            "hate_label": hate.get("label"),
            "hate_score": hate.get("score"),
            "sentiment_label": sentiment.get("label"),
            "sentiment_score": sentiment.get("score")
        }
<<<<<<< Updated upstream
        print(f"[RedditConsumer] Processed: {doc['title']} | Hate: {doc['hate_label']} | Sentiment: {doc['sentiment_label']}")
        res = es.index(index=index, body=doc)
        print(f"[RedditConsumer] Indexed to Elasticsearch: {doc['title']} (ID: {res['_id']})")
        print(f"[RedditConsumer] Data now visible in Kibana (index: {index})!")
        time.sleep(10)  # Wait 10 seconds before processing the next message for demonstration
=======

        # ✅ FIXED: Correct ES index reference
        try:
            res = es.index(index=config.es_index, document=enriched_post)
            print(f"[✅ RedditConsumer] Indexed to Elasticsearch (ID: {res['_id']})")
        except Exception as e:
            print(f"[ERROR] Failed to index document: {e}")

        time.sleep(1)
>>>>>>> Stashed changes

if __name__ == "__main__":
    main()
