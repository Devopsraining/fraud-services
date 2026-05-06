from fastapi import FastAPI
import random

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/predict")
def predict(data: dict):
    amount = data.get("amount", 0)

    # simple fraud logic
    fraud = 1 if amount > 5000 else 0

    return {
        "fraud": fraud,
        "confidence": round(random.random(), 2)
    }