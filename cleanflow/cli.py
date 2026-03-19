# cleanflow/cli.py

import argparse
import pandas as pd
import sys
import os

from cleanflow.pipeline.cleaner import CleanFlow
from cleanflow.utils.logger import get_logger
from cleanflow.init_config import load_data, generate_config, save_config

logger = get_logger("cleanflow.cli")


def main():
    parser = argparse.ArgumentParser(description="CleanFlow CLI")

    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser(
        "init", help="Generate config from dataset"
    )
    init_parser.add_argument(
        "input_file", help="Path to CSV or Excel file"
    )

    run_parser = subparsers.add_parser(
        "run", help="Run cleaning pipeline"
    )
    run_parser.add_argument("input_file", help="Path to dataset")
    run_parser.add_argument("config_file", help="Path to YAML config")
    run_parser.add_argument(
        "-o", "--output", help="Output file name (optional)"
    )
    run_parser.add_argument(
        "--strict",
        action="store_true",
        help="Override config and run in strict mode",
    )

    args = parser.parse_args()

    if args.command == "init":
        try:
            df = load_data(args.input_file)

            logger.info(f"Loaded dataset with {len(df.columns)} columns")

            config = generate_config(df)
            save_config(config)

        except Exception as e:
            logger.error(f"Init failed: {e}")
            sys.exit(1)


    elif args.command == "run":
        try:
            # load data
            df = load_data(args.input_file)
            logger.info(f"Loaded data from {args.input_file} ({len(df)} rows)")

            # initialize pipeline
            cleaner = CleanFlow(args.config_file)

            # override strict mode if CLI flag is used
            if args.strict:
                cleaner.strict_mode = True
                logger.info("Strict mode enabled via CLI")

            # run pipeline
            cleaned_df = cleaner.run(df)

            # determine output format
            output_path = args.output
            if not output_path:
                ext = os.path.splitext(args.input_file)[1].lower()
                if ext == ".csv":
                    output_path = "cleaned_output.csv"
                else:
                    output_path = "cleaned_output.xlsx"

            # save output
            if output_path.endswith(".csv"):
                cleaned_df.to_csv(output_path, index=False)
            elif output_path.endswith((".xls", ".xlsx")):
                cleaned_df.to_excel(output_path, index=False)
            else:
                raise ValueError("Unsupported output format. Use .csv or .xlsx")

            logger.info(f"Cleaned data saved to {output_path}")

        except FileNotFoundError as fe:
            logger.error(f"File not found: {fe}")
            sys.exit(1)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            sys.exit(1)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()