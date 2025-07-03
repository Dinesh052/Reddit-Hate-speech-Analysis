
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


# from transformers import pipeline
# from typing import Dict, Any

# class HateSpeechModel:
#     def __init__(self):
#         self.hate_classifier = pipeline(
#             "text-classification",
#              model="unitary/unbiased-toxic-roberta",
#              top_k=1
#         )
#         self.sentiment_classifier = pipeline(
#             "sentiment-analysis",
#             model="cardiffnlp/twitter-roberta-base-sentiment"
#         )

#     def hate_speech(self, text: str) -> Dict[str, Any]:
#         if not text or not isinstance(text, str):
#             return {"error": "Invalid input text."}
#         try:
#             result_list = self.hate_classifier(text)

#             # Handle nested list from top_k=1
#             if isinstance(result_list[0], list):
#                 result = result_list[0][0]
#             else:
#                 result = result_list[0]

#             # Map model label to readable label
#             label_map = {
#                 "toxicity": "offensive",
#                 "severe_toxicity": "hate",
#                 "identity_attack": "hate",
#                 "insult": "offensive",
#                 "obscene": "offensive",
#                 "threat": "hate",
#                 "sexual_explicit": "offensive"
#             }
#             mapped_label = label_map.get(result['label'], "neutral")

#             return {
#                 "label": mapped_label,
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

            model="Hate-speech-CNERG/dehatebert-mono-english",
            top_k=1

            model="unitary/unbiased-toxic-roberta",
            top_k=None  # Get all scores, not just top 1

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

            result = self.hate_classifier(text)[0]

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

            results = self.hate_classifier(text)[0]  # list of dicts
            scores = {r['label']: r['score'] for r in results}

            # Define category thresholds
            hate_threshold = 0.5
            offensive_threshold = 0.5

            # Priority: hate > offensive > neutral
            hate_labels = ['severe_toxicity', 'threat', 'identity_attack']
            offensive_labels = ['toxicity', 'obscene', 'insult', 'sexual_explicit']

            if any(scores.get(lbl, 0) > hate_threshold for lbl in hate_labels):
                hate_score = max(scores.get(lbl, 0) for lbl in hate_labels)
                return {"label": "hate", "score": round(hate_score, 3)}

            elif any(scores.get(lbl, 0) > offensive_threshold for lbl in offensive_labels):
                offensive_score = max(scores.get(lbl, 0) for lbl in offensive_labels)
                return {"label": "offensive", "score": round(offensive_score, 3)}

            else:
                return {"label": "neutral", "score": round(max(scores.values()), 3)}

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
