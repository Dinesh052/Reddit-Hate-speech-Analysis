import time
import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
from hate_speech_pipeline.config import Config
import praw
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")


# load_dotenv()
FETCH_INTERVAL = 10  # seconds

# Reddit app credentials (read-only access)
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_SECRET = os.getenv("REDDIT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "hate-speech-pipeline/0.1")

print("DEBUG -- CLIENT_ID:", REDDIT_CLIENT_ID)
print("DEBUG -- SECRET:", REDDIT_SECRET)
print("DEBUG -- USER_AGENT:", REDDIT_USER_AGENT)
# Public subreddits to fetch from
SUBREDDITS = [
    "HateSpeechOnTheWeb", "OffensiveSpeech", "SwearNet", "SwearWords"
]

def fetch_reddit_posts(reddit, subreddits):
    posts = []
    for sub in subreddits:
        print(f"[RedditProducer] Fetching from r/{sub}")
        for submission in reddit.subreddit(sub).new(limit=10):
            posts.append(submission)
            # Fetch all comments (including replies)
            submission.comments.replace_more(limit=None)
            for comment in submission.comments.list():
                posts.append(comment)
    return posts

def main():
    config = Config()
    producer = KafkaProducer(
        bootstrap_servers=config.kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

    print(f"[RedditProducer] Started. Fetching every {FETCH_INTERVAL} seconds.")
    while True:
        try:
            posts = fetch_reddit_posts(reddit, SUBREDDITS)
            print(f"[RedditProducer] Total posts fetched: {len(posts)}")

            for post in posts:
                if hasattr(post, 'title'):
                    data = {
                        "title": post.title,
                        "description": post.selftext,
                        "created_utc": post.created_utc,
                        "subreddit": post.subreddit.display_name,
                        "author": str(post.author),
                        "url": post.url,
                        "type": "submission"
                    }
                else:
                    data = {
                        "title": "",
                        "description": post.body,
                        "created_utc": post.created_utc,
                        "subreddit": post.subreddit.display_name,
                        "author": str(post.author),
                        "url": f"https://reddit.com{getattr(post, 'permalink', '')}",
                        "type": "comment"
                    }
                print(f"[RedditProducer] Sending: {data['description'][:60]}...")
                producer.send(config.kafka_topic, {"message": data})

            producer.flush()
            print(f"[RedditProducer] Sleeping for {FETCH_INTERVAL} seconds...\n")
            time.sleep(FETCH_INTERVAL)

        except Exception as e:
            print(f"[RedditProducer] Error: {e}")
            time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    main()
