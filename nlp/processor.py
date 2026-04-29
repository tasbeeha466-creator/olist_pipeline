import re

POSITIVE_WORDS = {
    "excelente", "otimo", "perfeito", "rapido", "recomendo", "amei",
    "adorei", "satisfeito", "parabens", "pontual", "lindo", "funciona",
    "qualidade", "gostei", "maravilhoso", "chegou", "bom", "rapida",
    "entregou", "certo", "correto", "recomendado", "feliz", "contente"
}

NEGATIVE_WORDS = {
    "pessimo", "horrivel", "problema", "defeito", "quebrado", "errado",
    "cancelado", "fraude", "estragado", "decepcionante", "ruim", "lento",
    "lenta", "triste", "insatisfeito", "nao funcionou", "parou", "falhou"
}

DELIVERY_COMPLAINT_WORDS = {
    "atraso", "atrasou", "nao recebi", "nao chegou", "demora", "demorou",
    "prazo", "entrega", "transportadora", "correios", "extraviado",
    "perdido", "sumiu", "nao apareceu", "atrasada", "atrasado"
}


def normalize(text):
    if not text or not isinstance(text, str):
        return ""
    text = text.lower()
    for pattern, replacement in [
        ("[áàãâä]", "a"), ("[éèêë]", "e"), ("[íìîï]", "i"),
        ("[óòõôö]", "o"), ("[úùûü]", "u"), ("[ç]", "c")
    ]:
        text = re.sub(pattern, replacement, text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return text.strip()


def score_sentiment(text):
    normalized = normalize(text)
    words = set(normalized.split())
    full_text = normalized
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    if neg > pos:
        return "negative"
    elif pos > neg:
        return "positive"
    return "neutral"


def is_delivery_complaint(text):
    normalized = normalize(text)
    return any(kw in normalized for kw in DELIVERY_COMPLAINT_WORDS)


def enrich_review(review_score, comment_message, comment_title):
    combined = f"{comment_title or ''} {comment_message or ''}"
    sentiment = score_sentiment(combined)
    delivery = is_delivery_complaint(combined)
    is_bad = review_score is not None and int(review_score) <= 3
    return {
        "sentiment": sentiment,
        "is_delivery_complaint": delivery,
        "is_bad_review": is_bad,
        "delivery_complaint_in_bad_review": is_bad and delivery
    }
