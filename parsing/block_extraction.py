import os
from const_regex import RE_INS, RE_SEL_OR_WITH, RE_CTAS_OR_VIEW, RE_TABLE_FROM_INS
from utils.str_utils import strip_leading_comments, strip_optimizer_hints

# ---------- Split statements ----------
def split_sql_statements(sql: str):
    """
        Cut at ; outside comments (-- and /* */),
    respecting parentheses.
    """
    stmts, buf = [], []
    in_single = in_double = False
    in_line_comment = in_block_comment = False
    depth = 0
    i, n = 0, len(sql)

    while i < n:
        ch = sql[i]
        nxt = sql[i+1] if i+1 < n else ''

        if in_line_comment:
            buf.append(ch)
            if ch == '\n':
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            buf.append(ch)
            if ch == '*' and nxt == '/':
                buf.append(nxt); i += 2
                in_block_comment = False
                continue
            i += 1
            continue
        if not in_single and not in_double:
            if ch == '-' and nxt == '-':
                buf.append(ch); buf.append(nxt); i += 2
                in_line_comment = True
                continue
            if ch == '/' and nxt == '*':
                buf.append(ch); buf.append(nxt); i += 2
                in_block_comment = True
                continue

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
            elif ch == ';' and depth == 0:
                buf.append(ch)
                stmts.append(''.join(buf).strip())
                buf.clear()
                i += 1
                continue

        buf.append(ch); i += 1

    rest = ''.join(buf).strip()
    if rest:
        stmts.append(rest)
    return [s for s in stmts if s]


def is_insert(stmt: str) -> bool:
    cleaned = strip_leading_comments(stmt)
    cleaned = strip_optimizer_hints(cleaned)
    return RE_INS.match(cleaned) is not None

def is_select_or_with(stmt: str) -> bool:
    head = strip_leading_comments(stmt)
    return RE_SEL_OR_WITH.match(head) is not None

def is_ctas_or_view(stmt: str) -> bool:
    head = strip_leading_comments(stmt)
    return RE_CTAS_OR_VIEW.match(head) is not None


def extract_model_blocks(content: str):
    """
    1 block = 1 statement (INSERT ... SELECT/WITH, INSERT ... VALUES, CTAS/VIEW AS, SELECT/WITH nu).
    """
    blocks = []
    for stmt in split_sql_statements(content):
        head = strip_leading_comments(stmt)
        if is_insert(head) or is_select_or_with(head) or is_ctas_or_view(head):
            if not stmt.endswith(";"):
                stmt += ";"
            blocks.append(stmt)
    return blocks


def table_name_from_block_or_filename(block: str, base_filename: str) -> str:
    """Define table name for dbt and Snowflake"""
    head = strip_leading_comments(block)
    head = strip_optimizer_hints(head)

    m = RE_TABLE_FROM_INS.match(head)
    if m:
        return m.group(1).split(".")[-1].strip('"')

    m2 = RE_CTAS_OR_VIEW.match(head)
    if m2:
        return m2.group("name").split(".")[-1].strip('"')

    stem = os.path.splitext(os.path.basename(base_filename))[0]
    u = stem.upper()
    for suf in ["_PROC", "_PROCEDURE", "_PRC", "_proc"]:
        if u.endswith(suf):
            stem = stem[:-len(suf)]
            u = stem.upper()
            break
    for pre in ["RECREATE_", "CREATE_"]:
        if u.startswith(pre):
            stem = stem[len(pre):]
            break
    return stem or "UNKNOWN_TABLE"
