import re
import gzip
import pandas as pd
from pathlib import Path

try:
    from rapidfuzz import process, fuzz
except ImportError:
    raise SystemExit("Installe rapidfuzz: pip install rapidfuzz")

IMDB_BASICS_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"  # IMDb Non-Commercial Datasets
# Détails officiels: schéma + colonne 'genres' dans title.basics.tsv.gz
# https://developer.imdb.com/non-commercial-datasets/

# ----------------------------
# 1) Typologie NORMALISÉE
# ----------------------------
# On mappe les genres IMDb (jusqu'à 3) vers 1 genre principal normalisé.
# (Tu peux ajuster selon ton besoin d'analyse TV.)
IMDB_TO_NORMALIZED = {
    "Animation": "Animation",
    "Comedy": "Comédie",
    "Drama": "Drame",
    "Crime": "Policier",
    "Action": "Action",
    "Adventure": "Aventure",
    "Fantasy": "Fantastique",
    "Sci-Fi": "Science-fiction",
    "History": "Historique",
    "Family": "Familial",
    "Romance": "Romance",
    "Thriller": "Thriller",
    "Horror": "Horreur",
    "Mystery": "Policier",   # souvent proche “policier/enquête”
    "War": "Historique",
    "Biography": "Drame",
    "Music": "Comédie",      # option: parfois Musical, à créer si tu veux
    "Musical": "Comédie",
    "Sport": "Drame",
    "Western": "Aventure",
    "Documentary": "Documentaire",
}

# Priorité pour choisir le “genre principal” quand IMDb donne 2-3 genres
NORMALIZED_PRIORITY = [
    "Animation", "Comédie", "Policier", "Action", "Aventure", "Science-fiction",
    "Fantastique", "Historique", "Drame", "Familial", "Romance", "Thriller",
    "Horreur", "Documentaire"
]

def normalize_title(s) -> str:
    if not isinstance(s, str):
        return ""
    s = s.upper().strip()
    s = re.sub(r"\s*\((L'|LE|LA|LES|UN|UNE|DES)\)\s*$", "", s)
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def pick_normalized(imdb_genres: str) -> str:
    if not imdb_genres or imdb_genres == r"\N":
        return ""
    imdb_list = [g.strip() for g in imdb_genres.split(",") if g.strip()]
    normalized = []
    for g in imdb_list:
        normalized.append(IMDB_TO_NORMALIZED.get(g, ""))

    normalized = [g for g in normalized if g]
    if not normalized:
        return ""

    # choisir selon priorité
    for pr in NORMALIZED_PRIORITY:
        if pr in normalized:
            return pr
    return normalized[0]

def download_if_needed(out_path: Path):
    if out_path.exists() and out_path.stat().st_size > 0:
        return
    import urllib.request
    print(f"Téléchargement IMDb: {IMDB_BASICS_URL}")
    urllib.request.urlretrieve(IMDB_BASICS_URL, out_path)
    print(f"OK: {out_path}")

def load_user_csv(path_csv: str) -> pd.DataFrame:
    # Ton fichier a des lignes “parasites” avant l’en-tête "titre;nb. de diffusions"
    lines = Path(path_csv).read_text(encoding="utf-8", errors="replace").splitlines()
    header_index = next(i for i, l in enumerate(lines) if l.lower().startswith("titre;"))
    header = [c for c in lines[header_index].split(";") if c]
    rows = [l.split(";")[:len(header)] for l in lines[header_index+1:] if l.strip()]
    df = pd.DataFrame(rows, columns=header)
    df["titre_norm"] = df["titre"].apply(normalize_title)
    return df

def build_imdb_index(imdb_gz_path: Path) -> pd.DataFrame:
    # On charge seulement les films (titleType=movie) + titres + genres + année
    # -> index en mémoire (suffisant pour 273 titres)
    print("Lecture IMDb title.basics.tsv.gz (filtrage movies)...")
    with gzip.open(imdb_gz_path, "rt", encoding="utf-8", errors="replace") as f:
        imdb = pd.read_csv(f, sep="\t", dtype=str)

    imdb = imdb[imdb["titleType"].isin(["movie"])]
    imdb["primary_norm"] = imdb["primaryTitle"].apply(normalize_title)
    imdb["original_norm"] = imdb["originalTitle"].apply(normalize_title)

    # On garde ce qu'il faut
    imdb = imdb[["tconst", "primaryTitle", "originalTitle", "startYear", "genres", "primary_norm", "original_norm"]]
    return imdb

def best_match(title_norm: str, imdb: pd.DataFrame, scorer=fuzz.WRatio):
    # candidates: on matche contre primary_norm et original_norm
    # On construit deux listes et on prend la meilleure.
    primary_list = imdb["primary_norm"].tolist()
    original_list = imdb["original_norm"].tolist()

    m1 = process.extractOne(title_norm, primary_list, scorer=scorer)
    m2 = process.extractOne(title_norm, original_list, scorer=scorer)

    # m = (match_string, score, index)
    if m2 and (not m1 or m2[1] > m1[1]):
        match_str, score, idx = m2
        row = imdb.iloc[idx].copy()
        row["match_field"] = "originalTitle"
    else:
        match_str, score, idx = m1
        row = imdb.iloc[idx].copy()
        row["match_field"] = "primaryTitle"

    row["match_score"] = score
    return row

def main(user_csv: str, out_csv: str, imdb_cache: str = "title.basics.tsv.gz", min_score: int = 88):
    user = load_user_csv(user_csv)

    imdb_path = Path(imdb_cache)
    download_if_needed(imdb_path)
    imdb = build_imdb_index(imdb_path)

    # match ligne par ligne
    matches = []
    for t in user["titre_norm"]:
        row = best_match(t, imdb)
        matches.append(row)

    match_df = pd.DataFrame(matches).reset_index(drop=True)
    user = user.reset_index(drop=True)

    # genre brut + normalisé
    user["imdb_genres_brut"] = match_df["genres"]
    user["genre"] = user["imdb_genres_brut"].apply(pick_normalized)

    # garde-fous: si score trop faible, on marque à vérifier
    user["match_score"] = match_df["match_score"]
    user["imdb_tconst"] = match_df["tconst"]
    user["imdb_title"] = match_df["primaryTitle"]
    user["imdb_year"] = match_df["startYear"]
    user["match_field"] = match_df["match_field"]

    user.loc[user["match_score"] < min_score, "genre"] = "A vérifier"
    user.to_csv(out_csv, sep=";", index=False, encoding="utf-8")
    print(f"OK -> {out_csv}")
    print("Résumé:")
    print(user["genre"].value_counts(dropna=False).head(20))

if __name__ == "__main__":
    # Exemple d'utilisation:
    main(
        user_csv="Films les plus diffusés à la télévision .csv",
        out_csv="Films_tv_avec_genre_normalise.csv",
        imdb_cache="title.basics.tsv.gz",
        min_score=88
    )
