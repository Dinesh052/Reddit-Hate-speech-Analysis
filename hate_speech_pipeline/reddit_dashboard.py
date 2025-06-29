from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from elasticsearch import Elasticsearch
import os
import re
from collections import Counter
import multiprocessing
import time

app = FastAPI()
templates = Jinja2Templates(directory="hate_speech_pipeline/templates")

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
INDEX = "reddit_hate"
es = Elasticsearch(ES_HOST)

SWEAR_WORDS = [
    "fuck", "shit", "bitch", "asshole", "bastard", "damn", "crap", "dick", "piss", "slut", "whore", "fag", "cunt", "bollocks", "bugger", "arse", "wanker", "prick", "twat", "slag"
]

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("reddit_dashboard.html", {"request": request})

@app.get("/api/posts")
def get_posts():
    res = es.search(index=INDEX, size=50, sort=[{"created_utc": {"order": "desc"}}])
    posts = [hit["_source"] for hit in res["hits"]["hits"]]
    return JSONResponse(posts)

@app.get("/api/stats")
def get_stats():
    res = es.search(index=INDEX, size=1000)
    posts = [hit["_source"] for hit in res["hits"]["hits"]]
    # Most used swear words (across all posts)
    word_counter = Counter()
    # Top swearing users (by total swear word count)
    user_swears = Counter()
    subreddit_counter = Counter()
    for post in posts:
        author = post.get("author", "unknown")
        text = (post.get("description") or "") + " " + (post.get("title") or "")
        words = re.findall(r"\\w+", text.lower())
        swears = [w for w in words if w in SWEAR_WORDS]
        word_counter.update(swears)
        if swears:
            user_swears[author] += len(swears)
        subreddit = post.get("subreddit", "unknown")
        subreddit_counter[subreddit] += 1
    return JSONResponse({
        "top_users": user_swears.most_common(10),
        "top_swears": word_counter.most_common(10),
        "top_subreddits": subreddit_counter.most_common(10)
    })

# --- Auto-start producer and consumer ---
def start_reddit_producer():
    from hate_speech_pipeline.reddit_producer import main as producer_main
    producer_main()

def start_reddit_consumer():
    from hate_speech_pipeline.reddit_consumer import main as consumer_main
    consumer_main()

if __name__ == "__main__":
    # Start producer and consumer in separate processes
    p1 = multiprocessing.Process(target=start_reddit_producer)
    p2 = multiprocessing.Process(target=start_reddit_consumer)
    p1.start()
    time.sleep(2)  # Give producer a head start
    p2.start()
    print("Reddit producer and consumer started. Run the dashboard with: uvicorn hate_speech_pipeline.reddit_dashboard:app --reload")
    p1.join()
    p2.join()
