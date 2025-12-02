import re
from const_regex import RE_JINJA_MACRO_CALL

def derive_cte_name_and_alias(macro_full_name: str) -> tuple[str, str, str]:
    """
    A partir du nom complet de macro 'module.func' ou 'func', dérive:
      - cte_name : 'cte_<func>'
      - alias    : 'm_<func>'
      - out_col  : '<func>_out'
    Toutes en snake_case. Ex: 'silver_funcs.get_country_code_func' ->
      ('cte_get_country_code_func', 'm_get_country_code_func', 'get_country_code_func_out')
    """
    func = macro_full_name.split('.')[-1]
    base = re.sub(r'[^A-Za-z0-9_]+', '_', func).lower()
    return f"cte_{base}", f"m_{base}", f"{base}_out"


def inject_macro_ctes(sql: str,
                      only_suffix: str = "_cte",
                      forced_macro_calls: list[str] | None = None) -> str:
    """
    Injection de CTE à partir :
      1) des macros présentes dans le SQL (filtrées par only_suffix si non vide),
      2) des macros *forcées* listées dans `forced_macro_calls` (injectées même si non présentes).

    Règles :
      - CTE = <base_name> AS {{ <call> }}, où base_name = dernier identifiant du nom de macro (sans module).
      - Si WITH existe -> on préprend nos CTE juste après 'WITH'.
      - Sinon -> on crée un 'WITH ...' en tête.
      - Anti-doublon : si '<base_name> AS {{' existe déjà, on n’injecte pas.
      - Ne fait rien si le SQL ne contient ni SELECT ni WITH.
    """
    # Normalisation de type
    if sql is None:
        return ""
    if not isinstance(sql, str):
        try:
            sql = sql.decode("utf-8") if isinstance(sql, (bytes, bytearray)) else str(sql)
        except Exception:
            return ""

    # Sans SELECT/WITH -> pas d’injection
    if not re.search(r"\b(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        return sql

    candidates: dict[str, str] = {}

    # 1) Macros présentes dans le SQL
    for m in RE_JINJA_MACRO_CALL.finditer(sql):
        full_name = m.group(1).strip()   # "macro" ou "module.macro"
        call      = m.group(0).strip()   # "{{ module.macro(args) }}"
        base_name = full_name.split(".")[-1]
        if only_suffix and not base_name.endswith(only_suffix):
            continue
        candidates.setdefault(base_name, call)

    # 2) Macros FORCÉES (liste d'appels)
    forced_macro_calls = forced_macro_calls or []
    for call in forced_macro_calls:
        mc = RE_JINJA_MACRO_CALL.search(call)
        if not mc:
            # Appel mal formé -> on ignore
            continue
        full_name = mc.group(1).strip()
        base_name = full_name.split(".")[-1]
        candidates.setdefault(base_name, call.strip())

    if not candidates:
        return sql

    # 3) Filtrer celles déjà définies comme CTE "<base_name> AS {{"
    to_inject: list[str] = []
    for base_name, call in candidates.items():
        if re.search(rf"\b{re.escape(base_name)}\b\s+as\s+\{{\{{", sql, flags=re.IGNORECASE):
            continue
        to_inject.append(f"{base_name} AS {call}")

    if not to_inject:
        return sql

    # 4) Injection en tête de WITH, ou création d'un WITH
    if re.match(r"^\s*with\b", sql, flags=re.IGNORECASE):
        m_head = re.match(r"^\s*with\s*", sql, flags=re.IGNORECASE)
        pos = m_head.end()
        injected = ",\n".join(to_inject) + ",\n"
        return sql[:pos] + injected + sql[pos:]
    else:
        return "WITH " + ",\n".join(to_inject) + "\n" + sql
    

def ensure_cte(sql: str, name: str, call: str) -> str:
    """
    Garantit la présence d'une CTE: <name> AS <call>.
    - Si déjà définie, ne fait rien.
    - Si WITH existe, préprend "name AS call," juste après WITH.
    - Sinon, crée un WITH ... avant le SELECT/WITH existant.
    """
    if sql is None:
        return ""
    if not isinstance(sql, str):
        sql = str(sql)

    # Déjà présente ?
    if re.search(rf"\b{name}\b\s+as\s+\{{\{{", sql, flags=re.IGNORECASE):
        return sql

    # Rien à faire si le body ne contient ni SELECT ni WITH
    if not re.search(r"\b(SELECT|WITH)\b", sql, flags=re.IGNORECASE):
        return sql

    if re.match(r"^\s*with\b", sql, flags=re.IGNORECASE):
        pos = re.match(r"^\s*with\s*", sql, flags=re.IGNORECASE).end()
        return sql[:pos] + f"{name} AS {call},\n" + sql[pos:]
    else:
        return f"WITH {name} AS {call}\n{sql}"
