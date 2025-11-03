from const_regex import PKG_FUNC_START, MACRO_NAMESPACE
from utils.navigation_sql import _find_matching_paren

def transform_pkg_functions_to_macros(sql: str) -> str:
    """
    Remplace tout appel à LH2_DTH_{SILVER|GOLD}_FUNCTIONS_PKG.<FUNC>(args)
    par {{ {silver|gold}_funcs.<func>(args) }} et ajoute un rappel en commentaire.
    """
    out = []
    i = 0
    n = len(sql)
    while i < n:
        m = PKG_FUNC_START.search(sql, i)
        if not m:
            out.append(sql[i:])
            break

        # ajouter la partie avant la fonction
        out.append(sql[i:m.start()])

        env = m.group(1).upper()   # SILVER | GOLD
        func = m.group(2)          # nom de la fonction
        ns = MACRO_NAMESPACE.get(env, env.lower() + "_funcs")

        # m.end() est après '(' -> position de '(' = m.end() - 1
        open_paren = m.end() - 1
        close_paren = _find_matching_paren(sql, open_paren)
        if close_paren == -1:
            out.append(sql[m.start():])
            break

        args_str = sql[open_paren + 1: close_paren]
        original_call = sql[m.start(): close_paren + 1]
        macro_call = f"{{{{ {ns}.{func.lower()}({args_str}) }}}} /* ORA_FUNC: {original_call} */"

        out.append(macro_call)
        i = close_paren + 1

    return "".join(out)
