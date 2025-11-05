import re

def extract_cte_names(sql):
    """Extracts all CTE names from a WITH clause using comma separation."""
    cte_names = set()
    with_match = re.search(r"(?is)\bWITH\s+(.*?)(?=\)\s*SELECT)", sql)  # (?i)(?s) = IGNORECASE + DOTALL
    if with_match:
        with_block = with_match.group(1)
        # Split entre CTEs, en ignorant la casse et en supportant une liste de colonnes optionnelle
        parts = re.split(r"(?i),\s*(?=[A-Za-z0-9_#]+(?:\s*\([^)]*\))?\s+AS\s*\()", with_block)
        for part in parts:
            # Nom de CTE + (liste de colonnes optionnelle) + AS(
            match = re.match(r"(?i)\s*([A-Za-z0-9_#]+)\s*(?:\([^)]*\))?\s+AS\s*\(", part.strip())
            if match:
                cte_names.add(match.group(1))
    return cte_names

def transform_table_references(sql, mode):
    """Transforms table references with the appropriate schemas, excluding CTEs."""
    cte_names = extract_cte_names(sql)
    print("cte_names:", cte_names)

    def replacer(match):
        keyword = match.group(1)
        table = match.group(2)

        if table in cte_names:
            return match.group(0)

        if table.lower() == 'steph_apps_fnd_flex_values#_bz':
            return f"{keyword} DEV.LH2_BRONZE_DEV.steph_apps_FND_FLEX_VALUES"

        if mode == "bronze to silver":
            if table.lower().endswith('_bz'):
                table_name = table[:-3]
                return f"{keyword} DEV.LH2_BRONZE_DEV.{table_name}"
            return f"{keyword} DEV.LH2_SILVER_DEV.{table}"

        if mode == "silver to gold":
            if table.lower().endswith('_sv'):
                table_name = table[:-3]
                return f"{keyword} DEV.LH2_SILVER_DEV.{table_name}"
            return f"{keyword} DEV.LH2_GOLD_DEV.{table}"

        return match.group(0)

    return re.sub(
        r"\b(FROM|JOIN|LEFT JOIN|LEFT OUTER JOIN|INNER JOIN|RIGHT JOIN|FULL JOIN)\s+([A-Za-z0-9_#]+)",
        replacer,
        sql,
        flags=re.IGNORECASE
    )