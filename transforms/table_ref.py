import re

#---------- Remove _bz ----------
def transform_table_references(sql, mode):
        """Transforms table references with the appropriate schemas"""
        def replacer(match):
            keyword = match.group(1)
            table = match.group(2)

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

        
        return re.sub(
            r"\b(FROM|JOIN)\s+([A-Za-z0-9_#]+)", 
            replacer, 
            sql, 
            flags=re.IGNORECASE
        )
