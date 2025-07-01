# from transformers import pipeline
# from typing import Dict, Any

# class HateSpeechModel:
#     def __init__(self):
#         self.hate_classifier = pipeline(
#             "text-classification",
#             model="Hate-speech-CNERG/dehatebert-mono-english",
#             top_k=1
#         )
#         self.sentiment_classifier = pipeline(
#             "sentiment-analysis",
#             model="cardiffnlp/twitter-roberta-base-sentiment"
#         )

#     def hate_speech(self, text: str) -> Dict[str, Any]:
#         if not text or not isinstance(text, str):
#             return {"error": "Invalid input text."}
#         try:
#             result = self.hate_classifier(text)[0]
#             return {
#                 "label": result['label'],
#                 "score": round(result['score'], 3)
#             }
#         except Exception as e:
#             return {"error": str(e)}

#     def sentiment(self, text: str) -> Dict[str, Any]:
#         if not text or not isinstance(text, str):
#             return {"error": "Invalid input text."}
#         try:
#             result = self.sentiment_classifier(text)[0]
#             return {
#                 "label": result['label'],
#                 "score": round(result['score'], 3)
#             }
#         except Exception as e:
#             return {"error": str(e)}


from transformers import pipeline
from typing import Dict, Any

class HateSpeechModel:
    def __init__(self):
        self.hate_classifier = pipeline(
            "text-classification",
             model="unitary/unbiased-toxic-roberta",
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
            result_list = self.hate_classifier(text)

            # Handle nested list from top_k=1
            if isinstance(result_list[0], list):
                result = result_list[0][0]
            else:
                result = result_list[0]

            # Map model label to readable label
            label_map = {
                "toxicity": "offensive",
                "severe_toxicity": "hate",
                "identity_attack": "hate",
                "insult": "offensive",
                "obscene": "offensive",
                "threat": "hate",
                "sexual_explicit": "offensive"
            }
            mapped_label = label_map.get(result['label'], "neutral")

            return {
                "label": mapped_label,
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
