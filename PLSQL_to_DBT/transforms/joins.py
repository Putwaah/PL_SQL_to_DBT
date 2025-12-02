import re
from const_regex import JINJA_CALL_RE
from utils.navigation_sql import _match_paren_balanced, find_top_level_keyword_positions
from utils.str_utils import mask_sql_comments_keep_layout, split_top_level_commas
from macros.cte_utils import derive_cte_name_and_alias

def split_top_level_and_spans(s: str):
    """
    Split par AND au niveau top-level, en renvoyant (texte, start, end).
    's' doit être déjà comment-maské pour éviter d'attraper des AND commentés.
    """
    parts = []
    buf = []
    in_single = in_double = False
    depth = 0
    n = len(s)
    i = 0
    seg_start = 0
    while i < n:
        # nouveau mot-clé AND top-level ?
        if not in_single and not in_double and depth == 0:
            if s[i:].lower().startswith('and') and (i == 0 or not s[i-1].isalnum()):
                j = i + 3
                if j >= n or not s[j].isalnum():
                    # flush segment courant
                    seg = ''.join(buf).strip()
                    if seg:
                        parts.append((seg, seg_start, i))
                    buf = []
                    # skip AND
                    i = j
                    # nouveau segment
                    while i < n and s[i].isspace():
                        i += 1
                    seg_start = i
                    continue
        ch = s[i]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
        buf.append(ch); i += 1
    seg = ''.join(buf).strip()
    if seg:
        parts.append((seg, seg_start, n))
    return parts

def _rewrite_joins_inside_derived_selects(sql: str,
                                          debug: bool = False,
                                          rewrite_inner: bool = True,
                                          drop_plus_constant_filters: bool = False) -> str:
    """
    Réécrit les jointures implicites à l'intérieur des sous-requêtes:
      FROM ( SELECT ... FROM A, B, C WHERE ... ) [PIVOT|...]
    en appelant récursivement rewrite_oracle_plus_joins() sur le SELECT interne.
    """
    if not sql or not isinstance(sql, str):
        return sql

    # Recherche incrémentale de "FROM ( SELECT"
    pattern = re.compile(r'\bFROM\s*\(\s*SELECT\b', flags=re.IGNORECASE)
    out, i, n = [], 0, len(sql)
    while i < n:
        m = pattern.search(sql, i)
        if not m:
            out.append(sql[i:])
            break

        # Début du motif "FROM ( SELECT"
        start = m.start()
        # On repère la parenthèse ouverte juste après "FROM"
        open_paren = sql.find('(', start, m.end())
        if open_paren == -1:
            # Sécurité : on recopie et on continue
            out.append(sql[i:m.end()])
            i = m.end()
            continue

        close_paren = _match_paren_balanced(sql, open_paren)
        if close_paren == -1:
            # Parenthèse non appariée -> on laisse tel quel
            out.append(sql[i:])
            break

        # Corps interne: "SELECT ...", y compris le mot-clé SELECT
        inner_select = sql[open_paren + 1: close_paren].strip()

        # Réécriture récursive du SELECT interne
        rewritten_inner = rewrite_oracle_plus_joins(
            inner_select,
            debug=debug,
            rewrite_inner=rewrite_inner,
            drop_plus_constant_filters=drop_plus_constant_filters,
        )

        # Reconstruction: on remplace le contenu entre parenthèses
        out.append(sql[i:open_paren + 1])
        out.append(rewritten_inner)
        i = close_paren  # on repart juste après le ')'

    return ''.join(out)

def rewrite_oracle_plus_joins(sql: str, debug: bool = False, rewrite_inner: bool = True, drop_plus_constant_filters: bool = False,) -> str:
    """
    Réécrit :
      - 'A = B(+)' -> LEFT/RIGHT JOIN ... ON ...
      - 'A = B' (implicite) -> INNER JOIN ... ON ... (si rewrite_inner=True)
      - Appels de macro Jinja dans une égalité -> injection d'un LEFT JOIN sur une CTE GENERIQUE
        dont le nom/alias/colonne sont dérivés du nom de la fonction :
           {{ module.func(arg) }} = T.col  ==>  LEFT JOIN cte_func m_func ON {{...}} = m_func.func_out
        Puis on remplace la macro par m_func.func_out pour permettre la jointure suivante.
      - Nettoie '(+)', '1=1', 'ON AND ...', 'WHERE ... AND' résiduels.
    """

    if not sql or not isinstance(sql, str):
        return sql

    
    sql = _rewrite_joins_inside_derived_selects(sql, debug=debug, rewrite_inner=rewrite_inner,drop_plus_constant_filters=drop_plus_constant_filters)


    # --- localiser FROM ... WHERE (niveau top) ---
    idx_from, idx_where = find_top_level_keyword_positions(sql, 'FROM', ['WHERE'])
    if idx_from < 0 or idx_where is None:
        return sql

    tail_kws = ['GROUP BY', 'ORDER BY', 'UNION']
    _, idx_tail = find_top_level_keyword_positions(sql[idx_where:], 'WHERE', tail_kws)
    end_where = (idx_where + idx_tail) if idx_tail is not None else len(sql)

    from_raw = sql[idx_from + len('FROM'): idx_where]
    where_raw = sql[idx_where + len('WHERE'): end_where]

    # --- masquer commentaires, on garde le layout ---
    from_mask = mask_sql_comments_keep_layout(from_raw)
    where_mask = mask_sql_comments_keep_layout(where_raw)

    # --- parse FROM items (actifs) ---
    items = split_top_level_commas(from_mask.strip())
    items = [it for it in items if it and not it.strip().startswith('--')]
    alias_to_text = {}
    aliases = set()
    for it in items:
        m = re.match(
            r'^\s*([A-Za-z0-9_."#]+(?:\s*\.\s*[A-Za-z0-9_."#]+)*)\s*(?:AS\s+)?([A-Za-z0-9_."#]+)?\s*$',
            it, flags=re.IGNORECASE
        )
        if not m:
            continue
        tbl = m.group(1).strip()
        alias = (m.group(2).strip() if m.group(2) else re.split(r'\s*\.\s*', tbl)[-1].strip('"'))
        alias_l = alias.lower()
        alias_to_text[alias_l] = it.strip()
        aliases.add(alias_l)

    if not items or not aliases:
        return sql

    # --- split WHERE conditions actives ---
    conds_spans = split_top_level_and_spans(where_mask)

    # groupement des joins
    join_groups: dict[tuple, list[str]] = {}
    consumed_spans = set()
    filters: list[str] = []
    extra_joins: list[str] = []  # JOIN CTE dérivées des macros

    def _alias_of(expr: str) -> str | None:
        for al in sorted(aliases, key=len, reverse=True):
            if re.search(rf'(?<![A-Za-z0-9_]){re.escape(al)}\s*\.', expr, flags=re.IGNORECASE):
                return al
        return None

    def _strip_leading_and(txt: str) -> str:
        return re.sub(r'^\s*and\b', '', txt, flags=re.IGNORECASE).strip()

    def _clean_side(s: str) -> str:
        return _strip_leading_and(s.replace('(+)',' ').strip())

    for _, c_start, c_end in conds_spans:
        raw = where_raw[c_start:c_end]
        raw_nocom = mask_sql_comments_keep_layout(raw).strip()

        # coupe sur '=' top-level
        in_single = in_double = False
        depth = 0
        eq_pos = -1
        for i, ch in enumerate(raw_nocom):
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif not in_single and not in_double:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth = max(0, depth - 1)
                elif ch == '=' and depth == 0:
                    eq_pos = i; break

        if eq_pos < 0:
            val = _strip_leading_and(raw_nocom)
            if val:
                filters.append(val)
                consumed_spans.add((c_start, c_end))
            continue

        lhs_raw = raw_nocom[:eq_pos].strip()
        rhs_raw = raw_nocom[eq_pos+1:].strip()
        lhs_plus = '(+)' in lhs_raw
        rhs_plus = '(+)' in rhs_raw

        lhs = _clean_side(lhs_raw)
        rhs = _clean_side(rhs_raw)

        # --- macro Jinja détectée ? ---
        m_left = JINJA_CALL_RE.search(lhs)
        m_right = JINJA_CALL_RE.search(rhs)
        if m_left or m_right:
            mj = m_left or m_right
            macro_full_name = mj.group(1)         # ex: silver_funcs.get_country_code_func
            macro_call = mj.group(0)              # ex: {{ silver_funcs.get_country_code_func(KDOHR.BILCOUNTRY) }}
            cte_name, cte_alias, out_col = derive_cte_name_and_alias(macro_full_name)

            # Injecte un LEFT JOIN sur la CTE générique : {{ macro(...) }} = cte_alias.out_col
            extra_joins.append(
                f"\nLEFT JOIN {cte_name} {cte_alias} ON {macro_call} = {cte_alias}.{out_col}"
            )
            # On remplace la macro par cte_alias.out_col pour la suite (afin que la condition restante soit joinable)
            if m_left:
                lhs = f"{cte_alias}.{out_col}"
            else:
                rhs = f"{cte_alias}.{out_col}"

            # Ajoute l'alias aux alias connus (sinon _alias_of ne le verra pas)
            aliases.add(cte_alias.lower())
            alias_to_text.setdefault(cte_alias.lower(), f"{cte_name} {cte_alias}")

        # Détection d'alias
        lhs_alias = _alias_of(lhs)
        rhs_alias = _alias_of(rhs)

        # Comparaison vers constante avec (+)
        if (lhs_alias and not rhs_alias or rhs_alias and not lhs_alias) and (lhs_plus or rhs_plus):
            if drop_plus_constant_filters:
                consumed_spans.add((c_start, c_end))
                continue
            filters.append(f"{lhs} = {rhs}")
            consumed_spans.add((c_start, c_end))
            continue

        # OUTER Oracle
        if lhs_alias and rhs_alias and lhs_alias != rhs_alias and (lhs_plus ^ rhs_plus):
            key = (lhs_alias, rhs_alias, 'LEFT') if (rhs_plus and not lhs_plus) else (rhs_alias, lhs_alias, 'RIGHT')
            join_groups.setdefault(key, []).append(f"{lhs} = {rhs}")
            consumed_spans.add((c_start, c_end))
            continue

        # INNER implicite
        if lhs_alias and rhs_alias and lhs_alias != rhs_alias:
            a, b = sorted([lhs_alias, rhs_alias])
            key = (a, b, 'INNER')
            join_groups.setdefault(key, []).append(f"{lhs} = {rhs}")
            consumed_spans.add((c_start, c_end))
            continue

        # sinon -> filtre
        filters.append(f"{lhs} = {rhs}")
        consumed_spans.add((c_start, c_end))

    # --- construction FROM + JOINs ---
    first_item = items[0].strip()
    m_first = re.match(
        r'^\s*(?:[A-Za-z0-9_."#]+(?:\s*\.\s*[A-Za-z0-9_."#]+)*)\s*(?:AS\s+)?([A-Za-z0-9_."#]+)?',
        first_item, flags=re.IGNORECASE
    )
    if m_first and m_first.group(1):
        base_alias = m_first.group(1).strip().strip('"').lower()
    else:
        base_alias = re.split(r'\s*\.\s*', first_item)[-1].strip('"').lower()

    from_parts = [first_item]
    if extra_joins:
        from_parts.extend(extra_joins)  # on met les JOIN-CTE dès le début
    joined = {base_alias}
    for j in extra_joins:
        # marque leurs alias comme rejoints
        m_al = re.search(r"\sJOIN\s+[A-Za-z0-9_\.\"#]+\s+([A-Za-z0-9_\"#]+)\s+ON", j, flags=re.IGNORECASE)
        if m_al:
            joined.add(m_al.group(1).strip('"').lower())

    remaining = dict(join_groups)
    progressed = True
    while progressed and remaining:
        progressed = False
        for key, cond_list in list(remaining.items()):
            a_al, b_al, jtype = key
            # on nettoie 'AND' en tête dans chaque condition
            conds = [re.sub(r'^\s*and\b', '', c, flags=re.IGNORECASE).strip() for c in cond_list]
            cond_txt = " AND ".join([c for c in conds if c])

            if jtype == 'LEFT':
                l_al, r_al = a_al, b_al
                if (l_al in joined) and (r_al not in joined):
                    tbl_txt = alias_to_text.get(r_al, r_al)
                    from_parts.append(f"\nLEFT JOIN {tbl_txt} ON {cond_txt}")
                    joined.add(r_al)
                    remaining.pop(key)
                    progressed = True

            elif jtype == 'RIGHT':
                l_al, r_al = a_al, b_al
                if (r_al in joined) and (l_al not in joined):
                    tbl_txt = alias_to_text.get(l_al, l_al)
                    from_parts.append(f"\nRIGHT JOIN {tbl_txt} ON {cond_txt}")
                    joined.add(l_al)
                    remaining.pop(key)
                    progressed = True

            elif jtype == 'INNER':
                if (a_al in joined) ^ (b_al in joined):
                    other = b_al if a_al in joined else a_al
                    tbl_txt = alias_to_text.get(other, other)
                    from_parts.append(f"\nINNER JOIN {tbl_txt} ON {cond_txt}")
                    joined.add(other)
                    remaining.pop(key)
                    progressed = True
                elif (a_al in joined) and (b_al in joined):
                    filters.append(cond_txt)
                    remaining.pop(key)
                    progressed = True

    # Fallback For the other join 
    for al_l, txt in alias_to_text.items():
        if al_l not in joined:
            from_parts.append(f"\n/*PROBLEME JOIN {txt}*/")
            joined.add(al_l)

    new_from = ' '.join(from_parts)

    # --- WHERE restant ---
    leftover = []
    for _, c_start, c_end in conds_spans:
        if (c_start, c_end) in consumed_spans:
            continue
        raw = where_raw[c_start:c_end]
        raw_nocom = mask_sql_comments_keep_layout(raw)
        val = re.sub(r'^\s*and\b', '', raw_nocom, flags=re.IGNORECASE).strip()
        if val:
            leftover.append(val)
    leftover.extend(filters)


    head = sql[:idx_from]
    tail = sql[end_where:]

    if leftover:
        clean = [re.sub(r'^\s*and\b', '', c, flags=re.IGNORECASE).strip() for c in leftover]
        clean = [c for c in clean if c]
        where_str = " AND\n  ".join(clean)
        rebuilt = f"{head}FROM {new_from}\nWHERE {where_str}\n{tail}"
    else:
        rebuilt = f"{head}FROM {new_from}\n{tail}"

    # Post-nettoyage de sécurité
    rebuilt = re.sub(r"(?i)\bON\s+AND\b", "ON ", rebuilt)               # ON AND -> ON
    rebuilt = re.sub(r"(?i)\bWHERE\s+1\s*=\s*1\s*(AND\s*)?", "WHERE ", rebuilt)
    rebuilt = re.sub(r"(?i)\bWHERE\s*(AND\s*)+\b", "WHERE ", rebuilt)   # WHERE AND -> WHERE
    rebuilt = re.sub(r"(?i)\s+AND\s*(AND\s*)+", " AND ", rebuilt)       # double AND
    rebuilt = re.sub(r"\bORDER\s+by\b", "ORDER BY", rebuilt)

    if debug:
        for (k1, k2, jt), conds in join_groups.items():
            print(f"[{jt} JOIN] {k1} <-> {k2}:")
            for c in conds:
                print(f"   ON {c}")

    return rebuilt