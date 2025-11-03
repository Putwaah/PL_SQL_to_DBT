import re

def move_order_by_outside_pivot(sql: str) -> str:
    """
    Déplace la clause ORDER BY située dans la sous-requête d'entrée d'un PIVOT
    jusqu'à la fin de la requête (après le PIVOT).
    
    Hypothèses :
      - Pattern ciblé : FROM ( SELECT ... ORDER BY ... ) PIVOT ( ... )
      - Gère plusieurs PIVOT dans le SQL.
      - Ne déplace pas si la sous-requête semble dépendre de l'ordre pour LIMIT/OFFSET (FETCH FIRST / OFFSET).
      
    Limitations :
      - Ne parse pas un SQL totalement arbitraire ; regex "raisonnables" pour le cas courant.
      - Pour Snowflake, si tu veux forcer un alias du résultat du PIVOT et préfixer les colonnes
        dans l'ORDER BY, vois la variante plus bas.
    """
    s = sql

    # Regex multi-occurrence : on capture FROM (SELECT ... ORDER BY ... ) PIVOT (
    pattern = re.compile(
        r"""
        (?P<prefix>                             # tout avant le FROM(
            ^|.*?
        )
        (?P<from>FROM\s*\(\s*SELECT\b)         # 'FROM ( SELECT'
        (?P<select_body>                        # corps du SELECT jusqu'à ORDER BY interne
            .*?
        )
        (?P<inner_order>\bORDER\s+BY\s+         # ORDER BY interne
            [^)]+                               # jusqu'à la parenthèse fermante de la sous-requête
        )
        (?P<after_subquery>\)\s*PIVOT\s*\()     # ') PIVOT ('
        """,
        flags=re.IGNORECASE | re.DOTALL | re.VERBOSE
    )

    # Pour éviter les boucles infinies, on remplace itérativement.
    pos = 0
    out = ""
    changed_any = False

    while True:
        m = pattern.search(s, pos)
        if not m:
            out += s[pos:]
            break

        # Copy ce qui précède le match
        out += s[pos:m.start()]

        from_kw = m.group('from')
        select_body = m.group('select_body')
        inner_order = m.group('inner_order')  # 'ORDER BY ...'
        after_subquery = m.group('after_subquery')  # ') PIVOT ('

        # Heuristique : si la sous-requête contient FETCH FIRST / OFFSET après ORDER BY, on ne touche pas
        sensitive = re.search(r'\b(FETCH\s+FIRST|OFFSET)\b', select_body + inner_order, flags=re.IGNORECASE)
        if sensitive:
            # on ne déplace pas → on recopie tel quel et on continue après ce pivot
            out += s[m.start():m.end()]
            pos = m.end()
            continue

        # Nettoyage : colonnes de l'ORDER BY interne (sans le mot-clé)
        inner_order_cols = re.sub(r'(?is)^\s*ORDER\s+BY\s+', '', inner_order).strip()

        # 1) Supprimer l'ORDER BY interne
        rebuilt = from_kw + select_body + after_subquery
        out += rebuilt

        # Mettre à jour le flux restant après ce point
        pos = m.end()

        # 2) On ajoutera l'ORDER BY à la fin (si pas déjà présent plus loin)
        marker = f"/*__PIVOT_ORDER_BY__:{inner_order_cols}__*/"
        out += marker
        changed_any = True

    final_sql = out

    if not changed_any:
        return sql  # rien n'a été modifié

    # Si un ORDER BY final existe déjà en dehors des markers, on n'ajoute rien.
    marker_re = re.compile(r"/\*__PIVOT_ORDER_BY__:(.*?)__\*/", flags=re.DOTALL)
    cols_sets = [m.group(1).strip() for m in marker_re.finditer(final_sql)]

    # On retire tous les markers du texte
    final_sql = marker_re.sub("", final_sql)

    # Déjà un ORDER BY final ? (en dehors des sous-requêtes typiques)
    already_final_order = re.search(r'\bORDER\s+BY\b[^;]*;?\s*$', final_sql.strip(), flags=re.IGNORECASE | re.DOTALL)

    if not already_final_order and cols_sets:
        # Aplatir toutes les colonnes en conservant l'ordre et en dédupliquant grossièrement
        flat_cols = []
        seen = set()
        for cols in cols_sets:
            # découpage simple par virgule (cas courants)
            for c in [x.strip() for x in cols.split(',') if x.strip()]:
                if c.lower() not in seen:
                    flat_cols.append(c)
                    seen.add(c.lower())

        # Insérer avant le ';' final s'il existe
        if final_sql.strip().endswith(';'):
            final_sql = final_sql.rstrip().rstrip(';').rstrip() + f"\n)ORDER BY {', '.join(flat_cols)};\n"
        else:
            final_sql = final_sql.rstrip() + f"\n)ORDER BY {', '.join(flat_cols)}\n"

    return final_sql
