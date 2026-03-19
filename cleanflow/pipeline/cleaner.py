# import necessary libs
import pandas as pd

from cleanflow.config.loader import load_config
from cleanflow.core import mapping
from cleanflow.utils.logger import get_logger
from cleanflow.utils.tracker import ChangeTracker

from cleanflow.validation.validator import (
    validate_config,
    validate_dataframe,
)

from cleanflow.core import text, types, missing, duplicates, outliers


logger = get_logger()


class CleanFlow:
    def __init__(self, config_path: str):
        self.config = load_config(config_path)

        # Validate config immediately
        validate_config(self.config)

        self.strict_mode = self.config.get("general", {}).get("strict_mode", False)
        self.tracker = ChangeTracker()

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting cleaning pipeline")
        
        # mapping columns
        df = mapping.map_columns(df, self.config, self.tracker)

        # validate incoming data
        validate_dataframe(df, self.config)

        try:
            # Step 1: Text Cleaning
            logger.info("Step 1: Text cleaning")
            df = text.clean_text(df, self.config, self.tracker)

            # Step 2: Type Fixing
            logger.info("Step 2: Type fixing")
            df = types.fix_types(df, self.config, self.tracker)

            # Step 3: Missing Values
            logger.info("Step 3: Handling missing values")
            df = missing.handle_missing(df, self.config, self.tracker)

            # Step 4: Outliers
            logger.info("Step 4: Handling outliers")
            df = outliers.handle_outliers(df, self.config, self.tracker)

            # Step 5: Duplicates
            logger.info("Step 5: Removing duplicates")
            df = duplicates.remove_duplicates(df, self.config, self.tracker)

        except Exception as e:
            if self.strict_mode:
                logger.error("❌ Pipeline failed in strict mode")
                raise
            else:
                logger.warning(f"⚠️ Pipeline error (continuing): {e}")

        logger.info("✅ Cleaning completed")

        # Print summary
        self._print_summary()

        return df

    def _print_summary(self):
        print("\n" + "=" * 40)
        print("🧼 CLEANFLOW SUMMARY")
        print("=" * 40)

        summary = self.tracker.summary()

        if summary:
            print(summary)
        else:
            print("No changes were made.")

        print("=" * 40)