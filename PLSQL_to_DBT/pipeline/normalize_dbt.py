import re
from utils.str_utils import split_top_level_commas, strip_leading_comments, strip_optimizer_hints, strip_tail_paren_and_semicolon, split_top_level_tuples
from const_regex import RE_INS_SEL_WITH, RE_INS_VALUES, RE_CTAS_OR_VIEW, RE_SEL_OR_WITH, RE_PARSE_INSERT_VALUES
from utils.defines import SILVER_LAYER, GOLD_LAYER

# ---------- DBT Standardization ----------

def normalize_block_for_dbt(block: str) -> str:
    """
    Extrait le SELECT/WITH utile pour dbt selon 4 cas :
      1) INSERT ... (SELECT|WITH) ...           -> on garde la partie SELECT/WITH
      2) INSERT ... VALUES (...)                 -> on convertit en SELECT ... UNION ALL ...
      3) CTAS/CREATE VIEW AS SELECT ...         -> on garde la partie SELECT/WITH
      4) SELECT/WITH nu                         -> on garde tel quel
    """
    # Nettoyage tête + hints pour fiabiliser les regex
    head = strip_leading_comments(block)
    head = strip_optimizer_hints(head)

    # 1) INSERT ... (SELECT|WITH)
    if RE_INS_SEL_WITH.match(head):
        ms = re.search(r"\bSELECT\b", head, flags=re.IGNORECASE)
        mw = re.search(r"\bWITH\b", head, flags=re.IGNORECASE)
        idx = None
        if ms and mw:
            idx = min(ms.start(), mw.start())
        elif ms:
            idx = ms.start()
        elif mw:
            idx = mw.start()
        if idx is not None:
            return strip_tail_paren_and_semicolon(head[idx:])

    # 2) INSERT ... VALUES
    if RE_INS_VALUES.match(head):
        return convert_insert_values_to_select_union(head)

    # 3) CTAS / VIEW AS
    m = RE_CTAS_OR_VIEW.match(head)
    if m:
        body = m.group("body")
        ms = re.search(r"\bSELECT\b", body, flags=re.IGNORECASE)
        mw = re.search(r"\bWITH\b", body, flags=re.IGNORECASE)
        idx = None
        if ms and mw:
            idx = min(ms.start(), mw.start())
        elif ms:
            idx = ms.start()
        elif mw:
            idx = mw.start()
        if idx is not None:
            return strip_tail_paren_and_semicolon(body[idx:])
        return strip_tail_paren_and_semicolon(body)

    # 4) SELECT / WITH nu
    if RE_SEL_OR_WITH.match(head):
        return head.strip().rstrip(";")

    # Fallback: on retourne quelque chose (évite None)
    return head.strip().rstrip(";")


def convert_insert_values_to_select_union(block: str) -> str:
    m = RE_PARSE_INSERT_VALUES.match(block)
    if not m:
        return block.strip().rstrip(";")

    cols_inner = m.group("cols_inner")
    values_str = m.group("values")

    tuples = split_top_level_tuples(values_str)
    if not tuples:
        return block.strip().rstrip(";")

    rows = [split_top_level_commas(t) for t in tuples]
    ncols = len(rows[0])
    if any(len(r) != ncols for r in rows):
        return block.strip().rstrip(";")

    if cols_inner:
        col_names = [c.strip() for c in split_top_level_commas(cols_inner)]
    else:
        col_names = [f"COL{i+1}" for i in range(ncols)]

    selects = []
    for idx, r in enumerate(rows):
        if idx == 0:
            pairs = [f"{r[j].strip()} AS {col_names[j]}" for j in range(ncols)]
        else:
            pairs = [r[j].strip() for j in range(ncols)]
        selects.append("SELECT " + ", ".join(pairs))
    return "\nUNION ALL\n".join(selects)



# ---------- Header dbt ----------
def generate_dbt_config(table_name: str, mode: str) -> str:
    """
    generate header for dbt
    """
    transient = "true" if "TEMP" in table_name.upper() else "false"

    schema = SILVER_LAYER
    if mode == "silver to gold":
        schema = GOLD_LAYER
    return f"""{{{{ config(
    materialized='table',
    transient={transient},
    schema='{schema}'
    alias='{table_name}'
) }}}}
"""


