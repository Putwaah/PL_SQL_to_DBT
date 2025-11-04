import re
from const_regex import RE_TRIPLE_ROWNUM_SYSDATE, RE_SELECT_LIST, RE_ROWNUM_ONE, RE_SYSDATE_CRE, RE_SYSDATE_UPD, RE_GROUP_BY_BLOCK, RE_SYS_ROW_GROUP_BY


def normalize_oracle_rownum_sysdate(sql: str) -> str:
    """
    Remplace ROWNUM/SYSDATE dans la liste SELECT par leurs équivalents Snowflake
    et supprime ROWNUM/SYSDATE du GROUP BY s'ils y figurent.

    - ROWNUM AS ROW_NUMBER_ID      -> seq8() + 1 AS ROW_NUMBER_ID
    - SYSDATE AS ROW_CREATION_DATE -> current_timestamp() AS ROW_CREATION_DATE
    - SYSDATE AS ROW_LAST_UPDATE_DATE -> current_timestamp() AS ROW_LAST_UPDATE_DATE

    Dans GROUP BY:
    - Supprime les items qui sont exactement "ROWNUM" ou "SYSDATE".
    - Si la liste devient vide après suppression, retire la clause GROUP BY.
    """
    def _triple_repl(m: re.Match) -> str:
        trailing = m.group('trailing') or ''
        return (
            " ROW_NUMBER() OVER (ORDER BY NULL) AS ROW_NUMBER_ID,\n"
            "current_timestamp() AS ROW_CREATION_DATE,\n"
            "current_timestamp() AS ROW_LAST_UPDATE_DATE" + trailing
        )

    def _rewrite_select_only(m: re.Match) -> str:
        select_list = m.group('select')

        s = RE_TRIPLE_ROWNUM_SYSDATE.sub(_triple_repl, select_list)

        s = RE_ROWNUM_ONE.sub("ROW_NUMBER() OVER (ORDER BY NULL)  AS ROW_NUMBER_ID", s)
        s = RE_SYSDATE_CRE.sub("current_timestamp() AS ROW_CREATION_DATE", s)
        s = RE_SYSDATE_UPD.sub("current_timestamp() AS ROW_LAST_UPDATE_DATE", s)

        return f"SELECT{s}FROM"

    sql2 = RE_SELECT_LIST.sub(_rewrite_select_only, sql)

    def _clean_group_by(m: re.Match) -> str:
        gb_text = m.group('gb')  # le contenu brut après "GROUP BY" et avant la clause suivante

        # Supprimer les items 'ROWWNUM' ou 'SYSDATE'
        cleaned = RE_SYS_ROW_GROUP_BY.sub(lambda mm: '' if mm.group(1).strip() == '' else mm.group(1), gb_text)

        cleaned = re.sub(r'\s*,\s*,\s*', ', ', cleaned)
        cleaned = re.sub(r'^\s*,\s*', '', cleaned)
        cleaned = re.sub(r'\s*,\s*$', '', cleaned)
        # - trim
        cleaned = cleaned.strip()

        # Si la liste est vide, supprimer la clause GROUP BY entière
        if cleaned == '':
            return ""

        # Sinon, reconstruire la clause intacte
        return f"GROUP BY {cleaned}"

    sql3 = RE_GROUP_BY_BLOCK.sub(_clean_group_by, sql2)


    return sql3
