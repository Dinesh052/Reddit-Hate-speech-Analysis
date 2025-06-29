from transformers import pipeline
from typing import Dict, Any

class HateSpeechModel:
    def __init__(self):
        self.hate_classifier = pipeline(
            "text-classification",
            model="Hate-speech-CNERG/dehatebert-mono-english",
            top_k=1
        )
        self.sentiment_classifier = pipeline(
            "sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment"
        )

    def hate_speech(self, text: str) -> Dict[str, Any]:
        if not text or not isinstance(text, str):
            return {"error": "Invalid input text."}
        try:
            result = self.hate_classifier(text)[0]
            return {
                "label": result['label'],
                "score": round(result['score'], 3)
            }
        except Exception as e:
            return {"error": str(e)}

    def sentiment(self, text: str) -> Dict[str, Any]:
        if not text or not isinstance(text, str):
            return {"error": "Invalid input text."}
        try:
            result = self.sentiment_classifier(text)[0]
            return {
                "label": result['label'],
                "score": round(result['score'], 3)
            }
        except Exception as e:
            return {"error": str(e)}
