import pandas as pd

from cleanflow.config.loader import load_config
from cleanflow.utils.logger import get_logger

from cleanflow.core.mapping import map_columns
from cleanflow.validation.validator import validate_dataframe, validate_config
from cleanflow.core.text import clean_text
from cleanflow.core.mapping_values import apply_value_mapping
from cleanflow.core.types import fix_types
from cleanflow.core.missing import standardize_missing, handle_missing
from cleanflow.core.outliers import handle_outliers
from cleanflow.core.duplicates import remove_duplicates
from cleanflow.utils.tracker import ChangeTracker


logger = get_logger(__name__)


class CleanFlow:
    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.strict_mode = self.config.get("general", {}).get("strict_mode", False)

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting cleaning pipeline")

        tracker = ChangeTracker()

        logger.info("Step 1: Column mapping")
        df = map_columns(df, self.config, tracker)

        logger.info("Step 2: Validation")
        validate_dataframe(df, self.config)

        logger.info("Step 3: Text cleaning")
        df = clean_text(df, self.config, tracker)

        logger.info("Step 4: Value mapping")
        df = apply_value_mapping(df, self.config, tracker)

        logger.info("Step 5: Type fixing")
        df = fix_types(df, self.config, tracker)
        
        logger.info("Step 6a: Standardizing missing value markers")
        df = standardize_missing(df, tracker=tracker)

        logger.info("Step 6b: Handling missing values")
        df = handle_missing(df, self.config, tracker)

        logger.info("Step 7: Handling outliers")
        df = handle_outliers(df, self.config, tracker)

        logger.info("Step 8: Removing duplicates")
        if self.config.get("duplicates", {}).get("drop"):
            df = remove_duplicates(df, self.config, tracker)

    
        logger.info("✅ Cleaning completed\n")
        tracker.summary()

        return df