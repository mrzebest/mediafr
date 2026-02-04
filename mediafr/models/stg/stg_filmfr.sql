{{ config(materialized = "table", schema = 'raw') }}

SELECT
-- normalisation des titres
CASE
    WHEN REGEXP_CONTAINS(
      titre,
      r"\s\((LE|LA|LES|L['’]|UN|UNE|DES|MON|MA|MES|TON|TA|TES)\)$"
    ) THEN
      CONCAT(
        REGEXP_EXTRACT(titre, r"\(([^)]+)\)$"),
        IF(REGEXP_CONTAINS(REGEXP_EXTRACT(titre, r"\(([^)]+)\)$"), r"['’]$"), "", " "),
        REGEXP_EXTRACT(titre, r"^(.*)\s\([^)]+\)$")
      )
    ELSE titre
  END AS titre,
r__alisateur_s_ 						AS realisatr,
DATE(ann__e_de_production,1,1) 			AS annee_productn,
groupe_de_nationalit__2 				AS pays_productn,
DATE(ann__e_de_derni__re_diffusion,1,1)	AS annee_derniere_diff,
nb__de_diffusions						AS nbr_diff,
nb__moyen_de_diffusions_par_an			AS nbr_moyen_diff_an,
genre,
match_score

FROM {{ source('filmfr','grid_external_filmfr') }}