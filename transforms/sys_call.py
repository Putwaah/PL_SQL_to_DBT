import re
from const_regex import RE_TRIPLE_ROWNUM_SYSDATE
def normalize_oracle_rownum_sysdate(sql: str) -> str:
    """
    Replaces the Oracle rownum and sysdate system calls with their Snowflake equivalents.
    """
    def _triple_repl(_m):
        return (
            "seq8() + 1 AS ROW_NUMBER_ID,\n "
            "current_timestamp() AS ROW_CREATION_DATE,\n "
            "current_timestamp() AS ROW_LAST_UPDATE_DATE\n"
        )

    #1) For the block of the 3rd var
    s = RE_TRIPLE_ROWNUM_SYSDATE.sub(_triple_repl, sql)

    # 2) Fallbacks if the 3 var is not together
    s = re.sub(
        r"\bROWNUM\b\s+(?:AS\s+)?ROW_NUMBER_ID\b",
        "seq8() + 1 AS ROW_NUMBER_ID",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"\bSYSDATE\b\s+(?:AS\s+)?ROW_CREATION_DATE\b",
        "current_timestamp() AS ROW_CREATION_DATE",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"\bSYSDATE\b\s+(?:AS\s+)?ROW_LAST_UPDATE_DATE\b",
        "current_timestamp() AS ROW_LAST_UPDATE_DATE",
        s,
        flags=re.IGNORECASE,
    )
    return s
