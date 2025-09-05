#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Embedding SQLAlchemy :
- Exécuté sans argument -> embed TOUTES les fiches (par lots).
- Fonction importable embed_one(id_etab) -> embed UNE fiche.
- CLI: --one-id <id> pour embed une fiche depuis la ligne de commande.

Dépendances:
  pip install sentence-transformers sqlalchemy psycopg2-binary numpy
"""

import os
import time
import math
from typing import List, Optional

import numpy as np
from sentence_transformers import SentenceTransformer

from sqlalchemy import select, func
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import NoSuchTableError, ProgrammingError

# =========================
# Import des modèles + engine depuis models.py
# =========================
engine: Optional[Engine] = None
Base = Etab = Review = EtabEmbedding = None  # seront remplis après import

try:
    # cas idéal: models.py expose get_engine()
    from models import Base, Etab, Review, EtabEmbedding, get_engine  # type: ignore
    engine = get_engine()
except Exception:
    try:
        # sinon, models.py expose "engine"
        from models import Base, Etab, Review, EtabEmbedding, engine as models_engine  # type: ignore
        engine = models_engine
    except Exception:
        # fallback: DATABASE_URL (ou PG_* défaut)
        from models import Base, Etab, Review, EtabEmbedding  # type: ignore
        from sqlalchemy import create_engine
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            PG_HOST = os.getenv("PG_HOST", "localhost")
            PG_PORT = os.getenv("PG_PORT", "5432")
            PG_DB   = os.getenv("POSTGRES_DB")
            PG_USER = os.getenv("POSTGRES_USER")
            PG_PASS = os.getenv("POSTGRES_PASSWORD")
            db_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        engine = create_engine(db_url, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# =========================
# Paramètres (ENV possibles)
# =========================
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "BAAI/bge-m3")
EMBED_DEVICE     = os.getenv("EMBED_DEVICE", "cpu")
BATCH_SIZE       = int(os.getenv("BATCH_ETABS", "200"))
EMBED_MAX_REV    = int(os.getenv("EMBED_MAX_REV", "5"))
RECOMPUTE_ALL    = os.getenv("EMBED_RECOMPUTE", "0") == "1"  # pour le mode 'tout'

# =========================
# Colonnes (en dur)
# =========================
# Etab
COL_ETAB_ID      = "id_etab"
COL_EDITORIAL    = "editorialSummary_text"
COL_DESC         = "description"
# Review
COL_REVIEW_ID    = "id_review"
COL_REVIEW_ETAB  = "id_etab"
COL_REVIEW_TEXT  = "original_text"
# EtabEmbedding
COL_EMB_ID       = "id_etab"
COL_EMB_DESC     = "desc_embed"
COL_EMB_REV      = "rev_embeds"
COL_EMB_TS       = "updated_at"

# =========================
# Helpers
# =========================
def log(msg: str) -> None:
    print(f"[embed] {msg}", flush=True)

_MODEL: Optional[SentenceTransformer] = None
def get_model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        log(f"Chargement du modèle {EMBED_MODEL_NAME} sur {EMBED_DEVICE}")
        _MODEL = SentenceTransformer(EMBED_MODEL_NAME, device=EMBED_DEVICE)
    return _MODEL

def encode_many(texts: List[str]) -> List[np.ndarray]:
    if not texts:
        return []
    arr = get_model().encode(
        texts, normalize_embeddings=True, show_progress_bar=False, device=EMBED_DEVICE
    )
    arr = np.asarray(arr, dtype=np.float32)
    if arr.ndim == 1:
        arr = arr[None, :]
    return [arr[i] for i in range(arr.shape[0])]

def ensure_tables(session: Session) -> None:
    Base.metadata.create_all(bind=session.get_bind())

def total_etabs(session: Session) -> int:
    return session.execute(select(func.count(getattr(Etab, COL_ETAB_ID)))).scalar_one()

def existing_embed_ids(session: Session) -> set:
    rows = session.execute(select(getattr(EtabEmbedding, COL_EMB_ID))).scalars().all()
    return set(rows)

def etab_batch(session: Session, offset: int, limit: int):
    stmt = (
        select(
            getattr(Etab, COL_ETAB_ID),
            func.coalesce(getattr(Etab, COL_EDITORIAL), ""),
            func.coalesce(getattr(Etab, COL_DESC), ""),
        )
        .order_by(getattr(Etab, COL_ETAB_ID))
        .offset(offset)
        .limit(limit)
    )
    return [(int(r[0]), r[1] or "", r[2] or "") for r in session.execute(stmt).all()]

def recent_reviews(session: Session, id_etab: int, limit: int) -> List[str]:
    try:
        stmt = (
            select(getattr(Review, COL_REVIEW_TEXT))
            .where(getattr(Review, COL_REVIEW_ETAB) == id_etab)
            .order_by(getattr(Review, COL_REVIEW_ID).desc())
            .limit(limit)
        )
        rows = session.execute(stmt).all()
    except (NoSuchTableError, ProgrammingError) as e:
        session.rollback()
        log(f"[DEBUG] reviews indisponibles pour etab {id_etab}: {e}")
        return []
    texts = [str(t[0]).strip() for t in rows if t and t[0]]
    return [t for t in texts if t]

def upsert_embedding(session: Session, id_etab: int, desc_vec: Optional[list], rev_vecs: Optional[list]) -> None:
    stmt = pg_insert(EtabEmbedding).values({
        COL_EMB_ID:   id_etab,
        COL_EMB_DESC: desc_vec,
        COL_EMB_REV:  rev_vecs,
    }).on_conflict_do_update(
        index_elements=[getattr(EtabEmbedding, COL_EMB_ID)],
        set_={
            COL_EMB_DESC: desc_vec,
            COL_EMB_REV:  rev_vecs,
            COL_EMB_TS:   func.now(),
        },
    )
    session.execute(stmt)

# =========================
# Fonctions publiques
# =========================
def embed_one(id_etab: int, recompute: bool = True) -> bool:
    """
    Embedding pour UN établissement.
    - recompute=True : recalcule même s'il existe déjà un embedding.
    Retourne True si un upsert a été fait, False sinon.
    """
    with SessionLocal() as session:
        ensure_tables(session)

        # Si on ne veut pas recalculer, vérifier l'existence
        if not recompute:
            exists = session.execute(
                select(getattr(EtabEmbedding, COL_EMB_ID)).where(getattr(EtabEmbedding, COL_EMB_ID) == id_etab)
            ).scalar_one_or_none()
            if exists is not None:
                log(f"id_etab {id_etab} déjà présent — ignoré (recompute=False).")
                return False

        # Récupérer les textes (editorial + description)
        row = session.execute(
            select(
                func.coalesce(getattr(Etab, COL_EDITORIAL), ""),
                func.coalesce(getattr(Etab, COL_DESC), "")
            ).where(getattr(Etab, COL_ETAB_ID) == id_etab)
        ).first()
        if not row:
            log(f"id_etab {id_etab} introuvable.")
            return False

        editorial, descr = row[0] or "", row[1] or ""
        base_text = ". ".join([t for t in (editorial, descr) if t])
        desc_vec = encode_many([base_text])[0].tolist() if base_text else None

        # Reviews
        texts = recent_reviews(session, id_etab, EMBED_MAX_REV)
        rev_vecs = [v.tolist() for v in encode_many(texts)] if texts else None

        # UPSERT
        upsert_embedding(session, id_etab, desc_vec, rev_vecs)
        session.commit()
        log(f"OK — embedding id_etab={id_etab} mis à jour.")
        return True

def embed_all(recompute: bool = RECOMPUTE_ALL, batch_size: int = BATCH_SIZE) -> None:
    """Embedding pour TOUS les établissements, par lots."""
    t0 = time.time()
    with SessionLocal() as session:
        ensure_tables(session)

        total = total_etabs(session)
        if total == 0:
            log("Aucun établissement.")
            return

        skip = existing_embed_ids(session) if not recompute else set()
        if skip:
            log(f"{len(skip)} embeddings déjà présents — ignorés (recompute=False).")

        pages = math.ceil(total / batch_size)
        processed = 0

        for p in range(pages):
            batch = etab_batch(session, offset=p * batch_size, limit=batch_size)
            if not batch:
                break

            upserts = 0
            for id_etab, editorial, descr in batch:
                if (not recompute) and (id_etab in skip):
                    continue

                base_text = ". ".join([t for t in (editorial, descr) if t])
                desc_vec = encode_many([base_text])[0].tolist() if base_text else None

                texts = recent_reviews(session, id_etab, EMBED_MAX_REV)
                rev_vecs = [v.tolist() for v in encode_many(texts)] if texts else None

                upsert_embedding(session, id_etab, desc_vec, rev_vecs)
                processed += 1
                upserts += 1

            session.commit()
            log(f"Page {p+1}/{pages} — upserts: {upserts} (cumul {processed}/{total})")

    log(f"Terminé en {time.time()-t0:.1f}s — {processed}/{total} établissements traités.")

# =========================
# CLI
# =========================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Embeddings établissements (tout / un seul).")
    parser.add_argument("--one-id", type=int, help="id_etab à (re)calculer uniquement")
    parser.add_argument("--no-recompute", action="store_true", help="ne pas recalculer si embedding déjà présent")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="taille de lot pour le mode 'tout'")
    args = parser.parse_args()

    if args.one_id is not None:
        embed_one(args.one_id, recompute=not args.no_recompute)
    else:
        embed_all(recompute=not args.no_recompute, batch_size=args.batch_size)
