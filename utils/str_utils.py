import re
from const_regex import RE_TRIPLE_ROWNUM_SYSDATE, RE_PO_BUYER_SCALAR, RE_INS,RE_INS_VALUES,RE_INS_SEL_WITH,RE_SEL_OR_WITH,RE_TABLE_FROM_INS,RE_CTAS_OR_VIEW,EXCLUDE_MACROS,RE_JINJA_MACRO_CALL,RE_PARSE_INSERT_VALUES,JINJA_CALL_RE,MACRO_NAMESPACE,PKG_FUNC_START



def split_top_level_commas(s: str):
    """Split par virgule au niveau top-level (hors quotes/parenthèses)."""
    parts, buf = [], []
    in_single = in_double = False
    depth = 0
    i, n = 0, len(s)
    while i < n:
        ch = s[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch); i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch); i += 1; continue
        if not in_single and not in_double:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(0, depth - 1)
            elif ch == ',' and depth == 0:
                parts.append(''.join(buf).strip())
                buf = []
                i += 1
                continue
        buf.append(ch); i += 1
    rest = ''.join(buf).strip()
    if rest:
        parts.append(rest)
    return parts

def split_top_level_tuples(values_str: str):
    """
    Split different procedure
    """
    tuples = []
    in_single = False
    esc = False
    depth = 0
    cur = []
    i = 0
    while i < len(values_str):
        ch = values_str[i]
        cur.append(ch)
        if ch == "\\" and not esc:
            esc = True
            i += 1
            continue
        if not esc:
            if ch == "'" and depth >= 0:
                in_single = not in_single
            elif not in_single:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        tuples.append("".join(cur).strip())
                        cur = []
                        j = i + 1
                        while j < len(values_str) and values_str[j].isspace():
                            j += 1
                        if j < len(values_str) and values_str[j] == ",":
                            j += 1
                            while j < len(values_str) and values_str[j].isspace():
                                j += 1
                            i = j - 1
        esc = False
        i += 1

    tail = "".join(cur).strip()
    if tail:
        tuples.append(tail)

    clean = []
    for t in tuples:
        t = t.strip().rstrip(",").strip()
        if t.startswith("(") and t.endswith(")"):
            clean.append(t[1:-1].strip())
    return clean

def mask_sql_comments_keep_layout(s: str) -> str:
    """
    Remplace le contenu des commentaires par des espaces en conservant la mise en page
    (même nombre de \n), pour garder les index alignés avec la chaîne originale.
    """
    if not s:
        return s
    out = []
    i = 0
    n = len(s)
    in_single = in_double = False
    while i < n:
        ch = s[i]
        nxt = s[i+1] if i+1 < n else ''
        # quotes
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch); i += 1; continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch); i += 1; continue
        # commentaires (hors quotes)
        if not in_single and not in_double:
            # -- line comment
            if ch == '-' and nxt == '-':
                # remplacer jusqu'au \n par des espaces, conserver le \n
                j = i + 2
                while j < n and s[j] != '\n':
                    out.append(' ')
                    j += 1
                out.append('\n' if j < n else '')
                i = j + 1
                continue
            # /* block comment */
            if ch == '/' and nxt == '*':
                j = i + 2
                while j + 1 < n and not (s[j] == '*' and s[j+1] == '/'):
                    out.append(' ' if s[j] != '\n' else '\n')
                    j += 1
                if j + 1 < n:
                    # ajouter '*/' masqué
                    out.append(' '); out.append(' ')
                    j += 2
                i = j
                continue
        out.append(ch); i += 1
    return ''.join(out)

def strip_leading_comments(s: str) -> str:
    """Delete comment (-- ... / /* ... */)."""
    i = 0
    n = len(s)
    while i < n:
        while i < n and s[i].isspace():
            i += 1
        if i + 1 < n and s[i:i+2] == "--":
            j = s.find("\n", i + 2)
            if j == -1:
                return ""
            i = j + 1
            continue
        if i + 1 < n and s[i:i+2] == "/*":
            j = s.find("*/", i + 2)
            if j == -1:
                return ""
            i = j + 2
            continue
        break
    return s[i:]

def strip_optimizer_hints(s: str) -> str:
    """
    Supprime les hints Oracle /*+ ... */ où qu'ils se trouvent dans le bloc.
    Non-greedy et multi-lignes.
    Ex: INSERT /*+ APPEND PARALLEL(t 8) */ INTO t ...
    """
    return re.sub(r"/\*\+.*?\*/", "", s, flags=re.DOTALL)

def strip_tail_paren_and_semicolon(sql: str) -> str:
    """Delete the ; and ) at the ending"""
    s = re.sub(r";\s*$", "", sql.rstrip())

    # Count parenthese depth
    len_sql = len(s)
    parenthese_depth = 0
    string = None
    i = 0
    while i < len_sql:
        if string != None:
            if string == s[i]:
                string = None
        else:
            if s[i] in ["'", '"']:
                string = s[i]
            elif s[i] == '(':
                parenthese_depth += 1
            elif s[i] == ')':
                parenthese_depth -= 1
        i += 1

    # If it's bellow 0, it mean that there is a close parenthese to remove
    if parenthese_depth < 0:
        s = re.sub(r"\)\s*$", "", s)
    return s.rstrip()
