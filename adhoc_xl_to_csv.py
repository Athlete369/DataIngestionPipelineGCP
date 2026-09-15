import os
import re
import pandas as pd
import gcsfs

fs = gcsfs.GCSFileSystem()

INPUT_FILE = "path/to/your/input/file.xlsx"
OUTPUT_FOLDER = "path/to/your/output/folder/"

def remove_empty_merged_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[
        :,
        ~df.columns.astype(str).str.contains(
            r"^Unnamed",
            case=False,
            na=False
        )
    ]
    df = df.dropna(axis=1, how="all")
    return df

def read_excel_file(file_path: str) -> pd.DataFrame:
    with fs.open(file_path, "rb") as excel_file:
        df = pd.read_excel(
            excel_file,
            skiprows=2,
            dtype=str,
            engine="openpyxl",
            )
    if df.empty:
        raise ValueError("Excel file is empty.")
    return df

def get_file_metadata(file_path: str) -> tuple[str, str]:
    file_name = os.path.basename(file_path)

    match = re.search(r'(\d{8})', file_name)
    file_date = match.group(1) if match else ""
    
    return file_name, file_date

def generate_output_file(input_file: str, output_folder: str) -> str:
    file_name = os.path.basename(input_file)
    base_name = os.path.splitext(file_name)[0]
    return f"{output_folder}{base_name}.csv"

def create_summary_df(file_name, file_date, record_count) -> pd.DataFrame:
    summary_data = {
        "file_name": [file_name],
        "file_date": [
            pd.to_datetime(
                file_date, 
                format="%Y%m%d"
            ).strftime("%Y-%m-%d")],
        "record_count": [float(record_count)]
    }
    return pd.DataFrame(summary_data)

def generate_summary_file(input_file: str, output_folder: str) -> str:
    file_name = os.path.basename(input_file)
    base_name = os.path.splitext(file_name)[0]

    return f"{output_folder}{base_name}_summary.csv"

def main(INPUT_FILE, OUTPUT_FOLDER):
    try:
        print(f"processing {INPUT_FILE}")
        return
        df = read_excel_file(INPUT_FILE)
        df = remove_empty_merged_columns(df)

        # Add filename and filedate as first two columns
        file_name, file_date = get_file_metadata(INPUT_FILE)
        df.insert(0, "file_name", file_name)
        df.insert(1, "file_date", file_date)

        output_file = generate_output_file(INPUT_FILE, OUTPUT_FOLDER)

        # Create detail CSV file
        with fs.open(output_file, "wb") as csv_file:
            df.to_csv(
                csv_file,
                index=False,
                encoding="utf-8",
                line_terminator="\n",
            )

        # Create summary CSV file
        summary_df = create_summary_df(
            file_name, 
            file_date, 
            len(df)
        )

        summary_file = generate_summary_file(
            INPUT_FILE,
            OUTPUT_FOLDER
        )

        with fs.open(summary_file, "wb") as csv_file:
            summary_df.to_csv(
                csv_file,
                index=False,
                encoding="utf-8-sig",
                line_terminator="\n",
            )

        print(f"Summary file created: {summary_file}")
        print(f"Created detail file: {output_file}")
        print(f"Rows processed: {len(df)}")
        print(f"Columns processed: {len(df.columns)}")


    except Exception as e:
        print("Processing failed.")
        print(f"Error processing {INPUT_FILE}: {e}")