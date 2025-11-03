def _is_kw_at(s: str, i: int, kw: str) -> bool:
    n = len(s)
    j = i + len(kw)
    before = s[i-1] if i > 0 else ' '
    after  = s[j]   if j < n else ' '
    return (s[i:j].lower() == kw.lower()
            and not (before.isalnum() or before == '_')
            and not (after.isalnum()  or after  == '_'))

def find_top_level_keyword_positions(sql: str, start_kw: str, next_kws: list[str]):
    s = sql
    n = len(s)
    start = start_kw.lower()
    nexts = [kw.lower() for kw in next_kws]
    in_single = in_double = False
    depth = 0
    i = 0
    found_from = -1

    while i < n:
        ch = s[i]
        ch2 = s[i:i+2]
        if ch == "'" and not in_double:
            in_single = not in_single; i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double; i += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif depth == 0:
                # *** ici : vérif stricte des frontières ***
                if _is_kw_at(s, i, start):
                    found_from = i
                    i += len(start)
                    break
        i += 1
    if found_from < 0:
        return -1, None

    # Suite inchangée, mais applique la même logique pour `next_kws`
    j = i
    best = None
    in_single = in_double = False
    depth = 0
    while j < n:
        ch = s[j]
        if ch == "'" and not in_double:
            in_single = not in_single; j += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double; j += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif depth == 0:
                for kw in nexts:
                    if _is_kw_at(s, j, kw):
                        best = j
                        return found_from, best
        j += 1
    return found_from, None

def _find_matching_paren(s: str, open_idx: int) -> int:
    """
    Retourne l'index de la parenthèse fermante qui matche s[open_idx] == '(',
    en respectant quotes simples/doubles et parenthèses imbriquées.
    -1 si non trouvé.
    """
    assert s[open_idx] == '(', "open_idx must point to '('"
    depth = 0
    in_single = in_double = False
    i = open_idx
    n = len(s)
    while i < n:
        ch = s[i]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return -1

def _match_paren_balanced(s: str, open_idx: int) -> int:
    """Renvoie l'index de la ')' qui matche s[open_idx] == '(' en respectant quotes et imbrications."""
    assert s[open_idx] == '('
    n = len(s)
    depth = 0
    in_single = False
    in_double = False
    i = open_idx
    while i < n:
        ch = s[i]
        nxt = s[i+1] if i+1 < n else ''
        if ch == "'" and not in_double:
            in_single = not in_single
            i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return -1
