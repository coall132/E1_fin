import hashlib, hmac, unicodedata, re, os

SECRET = os.environ.get("KEY_SALT")

def canon(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_review_key(author: str, text: str) -> str:
    msg = f"{canon(author)}|{canon(text)}".encode("utf-8")
    return hmac.new(SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()