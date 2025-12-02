import sys
import os
from runner.process import process_sql_file

def main():

    if len(sys.argv) != 2:
        print("Usage: python convert_to_dbt.py <path_to_sql_folder>")
        return

    input_folder = sys.argv[1]

    if not os.path.isdir(input_folder):
        print(f"Error: {input_folder} is not a valid directory.")
        return

    print("Choose the conversion layer :")
    print("1) Bronze → Silver")
    print("2) Silver → Gold")
    choice = input("1 or 2 : ").strip()

    if choice == "1":
        mode = "bronze to silver"
    elif choice == "2":
        mode = "silver to gold"
    else:
        print("Invalid number. Default : Bronze to Silver.")
        mode = "bronze to silver"

    output_dir = "dbt_models"
    os.makedirs(output_dir, exist_ok=True)

    total_files = 0
    converted_files = 0
    skipped_files = []

    for filename in os.listdir(input_folder):
        if filename.lower().endswith(".sql"):
            total_files += 1
            file_path = os.path.join(input_folder, filename)
            success = process_sql_file(file_path, output_dir, mode)
            if success:
                converted_files += 1
            else:
                skipped_files.append(filename)

    print("\n=== Conversion Summary ===")
    print(f"Total SQL files found: {total_files}")
    print(f"Successfully converted: {converted_files}")
    print(f"Skipped files: {len(skipped_files)}")
    if skipped_files:
        print("Skipped file list:")
        for fname in skipped_files:
            print(f" - {fname}")


if __name__ == "__main__":
    main()
