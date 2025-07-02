from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import Elasticsearch
import os
import re
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import multiprocessing
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Reddit Hate Speech Analytics", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="hate_speech_pipeline/templates")

# Configuration
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
INDEX = "reddit_hate"
es = Elasticsearch(ES_HOST)

SWEAR_WORDS = [
    "fuck", "shit", "bitch", "asshole", "bastard", "damn", "crap", "dick", 
    "piss", "slut", "whore", "fag", "cunt", "bollocks", "bugger", "arse", 
    "wanker", "prick", "twat", "slag"
]

CACHE_DURATION = 30  # seconds
cache = {
    "posts": [],
    "stats": {},
    "last_update": None,
    "insights": []
}

def is_cache_valid():
    if not cache["last_update"]:
        return False
    return (datetime.now() - cache["last_update"]).seconds < CACHE_DURATION

def update_cache(posts_data, stats_data):
    cache["posts"] = posts_data
    cache["stats"] = stats_data
    cache["last_update"] = datetime.now()
    cache["insights"] = generate_insights(posts_data)

def get_time_range_filter(time_range: str) -> Optional[datetime]:
    now = datetime.now()
    if time_range == "1h":
        return now - timedelta(hours=1)
    elif time_range == "6h":
        return now - timedelta(hours=6)
    elif time_range == "24h":
        return now - timedelta(hours=24)
    elif time_range == "7d":
        return now - timedelta(days=7)
    elif time_range == "30d":
        return now - timedelta(days=30)
    elif time_range == "365d" or time_range == "1y":
        return now - timedelta(days=365)
    elif time_range == "all":
        return None  # No time filter, return all data
    else:
        return now - timedelta(hours=24)

def categorize_hate_speech(label: str) -> str:
    if not label:
        return "neutral"
    label_lower = label.lower()
<<<<<<< Updated upstream
    if "hate" in label_lower:
        return "hate"
    elif "offensive" in label_lower:
        return "offensive"
    return "normal"
=======
    if label.lower() in ["hate", "offensive", "neutral"]:
        return label.lower()
    return "neutral"
    # if "hate" in label_lower or "threat" in label_lower:
    #     return "hate"
    # elif "offensive" in label_lower or "toxic" in label_lower or "obscene" in label_lower:
    #     return "offensive"
    # return "neutral"
>>>>>>> Stashed changes

def categorize_sentiment(label: str) -> str:
    if not label:
        return "neutral"
    label_lower = label.lower()
    if label_lower in ["positive", "label_2"]:
        return "positive"
    elif label_lower in ["negative", "label_0"]:
        return "negative"
    elif label_lower in ["neutral", "label_1"]:
        return "neutral"
    return "neutral"

def extract_swear_words(text: str) -> List[str]:
    if not text:
        return []
    words = re.findall(r'\b\w+\b', text.lower())
    return [word for word in words if word in SWEAR_WORDS]

def generate_insights(posts: List[Dict]) -> List[Dict]:
    if not posts:
        return []
    insights = []
    total_posts = len(posts)
    hate_posts = sum(1 for p in posts if categorize_hate_speech(p.get('hate_label')) == 'hate')
    hate_percentage = (hate_posts / total_posts * 100) if total_posts > 0 else 0
    if hate_percentage > 20:
        insights.append({
            "type": "warning",
            "title": "High Hate Speech Alert",
            "message": f"{hate_percentage:.1f}% of posts contain hate speech",
            "icon": "fas fa-exclamation-triangle",
            "color": "#da3633"
        })
    elif hate_percentage < 5:
        insights.append({
            "type": "success",
            "title": "Low Hate Speech",
            "message": f"Only {hate_percentage:.1f}% of posts contain hate speech",
            "icon": "fas fa-check-circle",
            "color": "#238636"
        })
    recent_posts = [p for p in posts if p.get('created_utc') and datetime.now().timestamp() - p['created_utc'] < 3600]
    if len(recent_posts) > 20:
        insights.append({
            "type": "info",
            "title": "High Activity Period",
            "message": f"{len(recent_posts)} posts in the last hour",
            "icon": "fas fa-chart-line",
            "color": "#1f6feb"
        })
    subreddit_counts = Counter(p.get('subreddit') for p in posts if p.get('subreddit'))
    if subreddit_counts:
        top_subreddit, top_count = subreddit_counts.most_common(1)[0]
        if top_count > total_posts * 0.3:
            insights.append({
                "type": "info",
                "title": "Dominant Subreddit",
                "message": f"r/{top_subreddit} accounts for {top_count} posts",
                "icon": "fas fa-hashtag",
                "color": "#ff6b6b"
            })
    sentiment_scores = [p.get('sentiment_score', 0) for p in posts if p.get('sentiment_score')]
    if sentiment_scores:
        avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
        if avg_sentiment > 0.6:
            insights.append({
                "type": "success",
                "title": "Positive Sentiment Trend",
                "message": f"Average sentiment score: {avg_sentiment:.2f}",
                "icon": "fas fa-smile",
                "color": "#238636"
            })
        elif avg_sentiment < 0.3:
            insights.append({
                "type": "warning",
                "title": "Negative Sentiment Trend",
                "message": f"Average sentiment score: {avg_sentiment:.2f}",
                "icon": "fas fa-frown",
                "color": "#d29922"
            })
    return insights

def calculate_average_sentiment(posts: List[Dict]) -> float:
    scores = [p.get("sentiment_score", 0) for p in posts if p.get("sentiment_score")]
    return round(sum(scores) / len(scores), 3) if scores else 0.0

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("reddit_dashboard.html", {"request": request})

@app.get("/api/posts")
async def get_posts(
    subreddit: Optional[str] = None,
    sentiment: Optional[str] = None,
    hate_category: Optional[str] = None,
    time_range: Optional[str] = "24h",
    limit: int = 100
):
    try:
        query = {"bool": {"must": []}}
        if subreddit and subreddit != "all":
            query["bool"]["must"].append({"term": {"subreddit.keyword": subreddit}})
        if sentiment and sentiment != "all":
            if sentiment == "positive":
                query["bool"]["must"].append({"wildcard": {"sentiment_label": "*pos*"}})
            elif sentiment == "negative":
                query["bool"]["must"].append({"wildcard": {"sentiment_label": "*neg*"}})
            elif sentiment == "neutral":
                query["bool"]["must"].append({"wildcard": {"sentiment_label": "*neu*"}})
        if hate_category and hate_category != "all":
            if hate_category == "hate":
                query["bool"]["must"].append({"wildcard": {"hate_label": "*hate*"}})
            elif hate_category == "offensive":
                query["bool"]["must"].append({"wildcard": {"hate_label": "*offensive*"}})
            elif hate_category == "neutral":
                query["bool"]["must"].append({"wildcard": {"hate_label": "*neutral*"}})
        res = es.search(
            index=INDEX, 
            body={"query": query},
            size=limit,
            sort=[{"created_utc": {"order": "desc"}}]
        )
        posts = []
        for hit in res["hits"]["hits"]:
            post = hit["_source"]
            post["hate_category"] = categorize_hate_speech(post.get("hate_label"))
            post["sentiment_category"] = categorize_sentiment(post.get("sentiment_label"))
            post["swear_words"] = extract_swear_words(post.get("description", "") + " " + post.get("title", ""))
            posts.append(post)
        if not any([subreddit, sentiment, hate_category]) and time_range == "24h":
            update_cache(posts, {})
        return JSONResponse(posts)
    except Exception as e:
        logger.error(f"Error fetching posts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
async def get_stats(time_range: str = "24h"):
    try:
        if is_cache_valid() and time_range == "24h":
            return JSONResponse(cache["stats"])
        time_filter = get_time_range_filter(time_range)
        query = {
            "bool": {
                "must": [{
                    "range": {"created_utc": {"gte": time_filter.timestamp()}}
                }]
            }
        }
        res = es.search(
            index=INDEX,
            body={"query": query},
            size=1000
        )
        posts = [hit["_source"] for hit in res["hits"]["hits"]]
        stats = {
            "total_posts": len(posts),
            "time_range": time_range,
            "last_updated": datetime.now().isoformat()
        }
        hate_counts = {"hate": 0, "offensive": 0, "neutral": 0}
        sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
        subreddit_counts = Counter()
        user_swear_counts = Counter()
        swear_word_counts = Counter()
        hourly_counts = defaultdict(lambda: {"hate": 0, "offensive": 0, "neutral": 0})
        for post in posts:
            hate_cat = categorize_hate_speech(post.get("hate_label"))
            hate_counts[hate_cat] += 1
            sentiment_cat = categorize_sentiment(post.get("sentiment_label"))
            sentiment_counts[sentiment_cat] += 1
            if post.get("subreddit"):
                subreddit_counts[post.get("subreddit")] += 1
            text = (post.get("description", "") + " " + post.get("title", "")).lower()
            swear_words = extract_swear_words(text)
            swear_word_counts.update(swear_words)
            if swear_words and post.get("author"):
                user_swear_counts[post.get("author")] += len(swear_words)
            if post.get("created_utc"):
                hour = datetime.fromtimestamp(post["created_utc"]).strftime("%H:00")
                hourly_counts[hour][hate_cat] += 1
        stats.update({
            "hate_distribution": hate_counts,
            "sentiment_distribution": sentiment_counts,
            "top_subreddits": subreddit_counts.most_common(10),
            "top_swearing_users": user_swear_counts.most_common(10),
            "top_swear_words": swear_word_counts.most_common(10),
            "hourly_breakdown": dict(hourly_counts),
            "hate_percentage": round((hate_counts["hate"] / max(stats["total_posts"], 1)) * 100, 2),
            "average_sentiment": calculate_average_sentiment(posts)
        })
        if time_range == "24h":
            update_cache(posts, stats)
        return JSONResponse(stats)
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/insights")
async def get_insights():
    if is_cache_valid():
        return JSONResponse(cache["insights"])
    # fallback: recalculate from latest posts
    res = es.search(index=INDEX, size=1000, sort=[{"created_utc": {"order": "desc"}}])
    posts = [hit["_source"] for hit in res["hits"]["hits"]]
    insights = generate_insights(posts)
    return JSONResponse(insights)

@app.get("/api/analytics/timeseries")
async def get_timeseries_data(time_range: str = "24h", interval: str = "1h"):
    try:
        time_filter = get_time_range_filter(time_range)
        if time_range == "1h":
            interval = "10m"
        elif time_range == "6h":
            interval = "30m"
        elif time_range == "24h":
            interval = "1h"
        else:
            interval = "1h"
        agg_query = {
            "query": {
                "bool": {
                    "must": [{
                        "range": {"created_utc": {"gte": time_filter.timestamp()}}
                    }]
                }
            },
            "aggs": {
                "posts_over_time": {
                    "date_histogram": {
                        "field": "created_utc",
                        "calendar_interval": interval,
                        "format": "yyyy-MM-dd HH:mm"
                    },
                    "aggs": {
                        "hate_posts": {
                            "filter": {"wildcard": {"hate_label": "*hate*"}}
                        },
                        "offensive_posts": {
                            "filter": {"wildcard": {"hate_label": "*offensive*"}}
                        }
                    }
                }
            },
            "size": 0
        }
        res = es.search(index=INDEX, body=agg_query)
        timeseries_data = []
        for bucket in res["aggregations"]["posts_over_time"]["buckets"]:
            timeseries_data.append({
                "timestamp": bucket["key_as_string"],
                "total": bucket["doc_count"],
                "hate": bucket["hate_posts"]["doc_count"],
                "offensive": bucket["offensive_posts"]["doc_count"],
                "normal": bucket["doc_count"] - bucket["hate_posts"]["doc_count"] - bucket["offensive_posts"]["doc_count"]
            })
        return JSONResponse(timeseries_data)
    except Exception as e:
        logger.error(f"Error fetching timeseries data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
