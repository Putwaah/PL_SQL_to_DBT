import re

# --- REPLACE ROWNUM / SYSDATE ---
RE_TRIPLE_ROWNUM_SYSDATE = re.compile(r"""
    \bROWNUM\b \s+ (?:AS\s+)?ROW_NUMBER_ID \s* , \s*
    \bSYSDATE\b \s+ (?:AS\s+)?ROW_CREATION_DATE \s* , \s*
    \bSYSDATE\b \s+ (?:AS\s+)?ROW_LAST_UPDATE_DATE
""", re.IGNORECASE | re.VERBOSE | re.DOTALL)

# Remplace la macro scalaire par la colonne issue de la CTE
RE_PO_BUYER_SCALAR = re.compile(
    r"""\{\{[^}]*get_steph_apps_per_all_people_f_name_func\s*\([^)]*\)\s*\}\}
        \s*(?:/\*.*?\*/\s*)?
        (?:AS\s+)?PO_BUYER\b
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

# --- Bloc Detection (robuste) ---
RE_INS = re.compile(r"^\s*INSERT\s+INTO\s+", re.IGNORECASE | re.DOTALL)
RE_INS_VALUES = re.compile(r"^\s*INSERT\s+INTO\s+.+?\bVALUES\b", re.IGNORECASE | re.DOTALL)
RE_INS_SEL_WITH = re.compile(r"^\s*INSERT\s+INTO\s+.+?\b(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)
RE_SEL_OR_WITH = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE | re.DOTALL)

# INSERT INTO <table> (schéma/quotes autorisés)
RE_TABLE_FROM_INS = re.compile(
    r"""^\s*INSERT\s+INTO\s+
        (
          (?:"[^"]+"|[A-Za-z0-9_#]+)
          (?:\.(?:"[^"]+"|[A-Za-z0-9_#]+))?
        )
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

RE_CTAS_OR_VIEW = re.compile(
    r"""^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:(?:TRANSIENT|TEMPORARY|TEMP)\s+)?(?P<kind>TABLE|VIEW)\s+
        (?P<name>[^\s(]+)\s+AS\s+(?P<body>.+)$
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)


# Macros à exclure systématiquement de l'injection en CTE (tu peux adapter)
EXCLUDE_MACROS = {
    "config", "ref", "source", "var", "env_var"  # primitives dbt usuelles
}


RE_JINJA_MACRO_CALL = re.compile(
    r"\{\{\s*([A-Za-z_][A-Za-z0-9_\.]*)\s*\(([^}]*)\)\s*\}\}",
    re.DOTALL
)

# ---------- Conversion INSERT VALUES -> SELECT UNION ALL ----------
RE_PARSE_INSERT_VALUES = re.compile(
    r"""^\s*INSERT\s+INTO\s+
        (?P<table>[^\s(]+)
        \s*
        (?P<cols>\((?P<cols_inner>.*?)\))?
        \s*VALUES\s*(?P<values>.+);?\s*$""",
    re.IGNORECASE | re.DOTALL | re.VERBOSE
)

    # Jinja macro : {{ module.func(args) }}
JINJA_CALL_RE = re.compile(r"\{\{\s*([A-Za-z_][\w\.]*)\s*\((.*?)\)\s*\}\}")


MACRO_NAMESPACE = {
    'SILVER': 'silver_funcs',
    'GOLD':   'gold_funcs',
}

PKG_FUNC_START = re.compile(
    r"\bLH2_DTH_(SILVER|GOLD)_FUNCTIONS_PKG\.([A-Za-z0-9_]+)\s*\(",
    re.IGNORECASE
)
