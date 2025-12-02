import os
from parsing.block_extraction import extract_model_blocks, table_name_from_block_or_filename
from pipeline.normalize_dbt import generate_dbt_config, normalize_block_for_dbt
from transforms.pkg_function import transform_pkg_functions_to_macros
from transforms.joins import rewrite_oracle_plus_joins
from transforms.pivot import move_order_by_outside_pivot
from transforms.table_ref import transform_table_references
from transforms.sys_call import normalize_oracle_rownum_sysdate


# ---------- Traitement d'un fichier ----------
def process_sql_file(file_path: str, output_dir: str, mode: str) -> bool:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="latin-1") as f:
                content = f.read()
        except Exception as e:
            print(f"[ERROR] {file_path}: Failed to read file due to encoding issue - {e}")
            return False

    blocks = extract_model_blocks(content)
    if not blocks:
        print(f"[SKIPPED] {os.path.basename(file_path)}: No INSERT/SELECT/CTAS/VIEW block found.")
        return False

    base_filename = os.path.basename(file_path)
    stem = os.path.splitext(base_filename)[0]
    os.makedirs(output_dir, exist_ok=True)

    tables_file = dict()

    for i, raw_block in enumerate(blocks, start=1):
        table_name = table_name_from_block_or_filename(raw_block, base_filename).upper()
        header = generate_dbt_config(table_name, mode)

        # 1) Normalisation principale
        body = normalize_block_for_dbt(raw_block)

        # 2) Remplacement package -> macros scalaires
        body = transform_pkg_functions_to_macros(body)
        body = rewrite_oracle_plus_joins(body)
        body = move_order_by_outside_pivot(body)

        # 3) Suite (réécriture FROM/JOIN + normalisation)
        body = transform_table_references(body, mode)
        body = normalize_oracle_rownum_sysdate(body)

        # Si la table n'est dans aucun fichier, on lui créer son fichier
        if table_name not in tables_file.keys():
            # 4) Création du fichier finale et du nom de fichier
            dbt_model = header + "\n" + body + "\n"
            out_name = f"{table_name}.sql"
            out_path = os.path.join(output_dir, out_name)
            tables_file[table_name] = out_path

            # 5) Écriture dans le fichier
            try:
                with open(out_path, "w", encoding="utf-8") as out_f:
                    out_f.write(dbt_model)
            except Exception as e:
                print(f"[ERROR] {out_name}: Failed to write output file - {e}")

        # Sinon on récupère le fichier de la table et on ajoute un autre body
        else:
            # 4) Création du fichier finale et du nom de fichier
            dbt_model = "\n\nUNION ALL\n\n" + body
            out_path = tables_file[table_name]

            # 5) Écriture dans le fichier
            try:
                with open(out_path, "a", encoding="utf-8") as out_f:
                    out_f.write(dbt_model)
            except Exception as e:
                print(f"[ERROR] {out_name}: Failed to write output file - {e}")
    return True
