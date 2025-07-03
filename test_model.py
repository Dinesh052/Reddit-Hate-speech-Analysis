from hate_speech_pipeline.model import HateSpeechModel

model = HateSpeechModel()

# Hateful text to test
text = "Kill all people of that race."

# Run prediction
result = model.hate_speech(text)

print("Input text:", text)
print("Hate speech prediction:", result)
