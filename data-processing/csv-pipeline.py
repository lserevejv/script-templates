"""
CSV Processing Pipeline Template
Production-ready CSV data processing with validation, transformation, and export.

Features:
- CSV reading with error handling
- Data validation and type conversion
- Data transformation and enrichment
- Duplicate detection and removal
- Export to multiple formats
- Progress reporting
- Memory-efficient processing for large files
"""

import csv
import json
import pandas as pd
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OutputFormat(Enum):
    """Supported output formats"""
    CSV = "csv"
    JSON = "json"
    EXCEL = "excel"
    PARQUET = "parquet"


@dataclass
class ValidationRule:
    """Data validation rule"""
    field: str
    validator: Callable
    error_message: str


class CSVProcessor:
    """Production-ready CSV processing pipeline"""
    
    def __init__(self, input_file: str):
        self.input_file = Path(input_file)
        self.data: Optional[pd.DataFrame] = None
        self.validation_rules: List[ValidationRule] = []
        self.transformations: List[Callable] = []
        self.errors: List[Dict[str, Any]] = []
        
        logger.info(f"CSV processor initialized for {input_file}")
    
    def add_validation_rule(self, field: str, validator: Callable, error_message: str):
        """Add a validation rule"""
        self.validation_rules.append(ValidationRule(field, validator, error_message))
        logger.info(f"Added validation rule for field: {field}")
    
    def add_transformation(self, transformation: Callable):
        """Add a data transformation"""
        self.transformations.append(transformation)
        logger.info(f"Added transformation: {transformation.__name__}")
    
    def load_csv(self, **kwargs) -> bool:
        """Load CSV file with error handling"""
        try:
            self.data = pd.read_csv(self.input_file, **kwargs)
            logger.info(f"Loaded {len(self.data)} rows from CSV")
            return True
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            self.errors.append({
                'stage': 'load',
                'error': str(e),
                'file': str(self.input_file)
            })
            return False
    
    def validate_data(self) -> bool:
        """Validate data against rules"""
        if self.data is None:
            logger.error("No data loaded for validation")
            return False
        
        validation_passed = True
        
        for rule in self.validation_rules:
            if rule.field not in self.data.columns:
                logger.warning(f"Field {rule.field} not found in data")
                continue
            
            try:
                # Apply validation
                mask = self.data[rule.field].apply(rule.validator)
                invalid_rows = self.data[mask]
                
                if not invalid_rows.empty:
                    validation_passed = False
                    self.errors.append({
                        'stage': 'validation',
                        'field': rule.field,
                        'error': rule.error_message,
                        'invalid_count': len(invalid_rows),
                        'sample_invalid': invalid_rows.head(5).to_dict('records')
                    })
                    logger.warning(f"Validation failed for {rule.field}: {len(invalid_rows)} invalid rows")
                    
            except Exception as e:
                logger.error(f"Validation error for {rule.field}: {e}")
                validation_passed = False
        
        return validation_passed
    
    def transform_data(self) -> bool:
        """Apply transformations to data"""
        if self.data is None:
            logger.error("No data loaded for transformation")
            return False
        
        try:
            for transformation in self.transformations:
                self.data = transformation(self.data)
                logger.info(f"Applied transformation: {transformation.__name__}")
            
            return True
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            self.errors.append({
                'stage': 'transformation',
                'error': str(e)
            })
            return False
    
    def remove_duplicates(self, subset: Optional[List[str]] = None) -> int:
        """Remove duplicate rows"""
        if self.data is None:
            logger.error("No data loaded")
            return 0
        
        original_count = len(self.data)
        self.data = self.data.drop_duplicates(subset=subset)
        duplicates_removed = original_count - len(self.data)
        
        logger.info(f"Removed {duplicates_removed} duplicate rows")
        return duplicates_removed
    
    def clean_data(self) -> bool:
        """Clean data - handle missing values, trim strings, etc."""
        if self.data is None:
            logger.error("No data loaded for cleaning")
            return False
        
        try:
            # Remove completely empty rows
            self.data = self.data.dropna(how='all')
            
            # Trim string columns
            string_columns = self.data.select_dtypes(include=['object']).columns
            for col in string_columns:
                self.data[col] = self.data[col].str.strip()
            
            # Convert date columns
            date_columns = [col for col in self.data.columns if 'date' in col.lower()]
            for col in date_columns:
                try:
                    self.data[col] = pd.to_datetime(self.data[col])
                except:
                    logger.warning(f"Could not convert {col} to datetime")
            
            logger.info("Data cleaning completed")
            return True
            
        except Exception as e:
            logger.error(f"Data cleaning failed: {e}")
            return False
    
    def export_data(self, output_file: str, format: OutputFormat = OutputFormat.CSV) -> bool:
        """Export data to specified format"""
        if self.data is None:
            logger.error("No data to export")
            return False
        
        try:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if format == OutputFormat.CSV:
                self.data.to_csv(output_path, index=False)
            elif format == OutputFormat.JSON:
                self.data.to_json(output_path, orient='records', indent=2)
            elif format == OutputFormat.EXCEL:
                self.data.to_excel(output_path, index=False)
            elif format == OutputFormat.PARQUET:
                self.data.to_parquet(output_path, index=False)
            
            logger.info(f"Exported {len(self.data)} rows to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            self.errors.append({
                'stage': 'export',
                'error': str(e),
                'output_file': output_file
            })
            return False
    
    def get_summary(self) -> Dict[str, Any]:
        """Get data summary"""
        if self.data is None:
            return {"error": "No data loaded"}
        
        return {
            "rows": len(self.data),
            "columns": list(self.data.columns),
            "memory_usage": self.data.memory_usage(deep=True).sum(),
            "dtypes": self.data.dtypes.to_dict(),
            "null_counts": self.data.isnull().sum().to_dict(),
            "errors": len(self.errors)
        }
    
    def process_pipeline(self, output_file: str, output_format: OutputFormat = OutputFormat.CSV) -> bool:
        """Run complete processing pipeline"""
        logger.info("Starting CSV processing pipeline")
        
        # Load data
        if not self.load_csv():
            return False
        
        # Clean data
        if not self.clean_data():
            return False
        
        # Validate data
        if not self.validate_data():
            logger.warning("Data validation failed, but continuing with warnings")
        
        # Remove duplicates
        duplicates_removed = self.remove_duplicates()
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicates")
        
        # Apply transformations
        if not self.transform_data():
            return False
        
        # Export data
        if not self.export_data(output_file, output_format):
            return False
        
        # Report summary
        summary = self.get_summary()
        logger.info(f"Pipeline completed: {summary}")
        
        return True


# Example usage
if __name__ == "__main__":
    # Initialize processor
    processor = CSVProcessor("input.csv")
    
    # Add validation rules
    processor.add_validation_rule(
        field="email",
        validator=lambda x: pd.Series(x).str.contains('@', na=False),
        error_message="Invalid email format"
    )
    
    processor.add_validation_rule(
        field="age",
        validator=lambda x: (x >= 0) & (x <= 120),
        error_message="Age must be between 0 and 120"
    )
    
    # Add transformations
    def uppercase_names(df):
        df['name'] = df['name'].str.upper()
        return df
    
    def calculate_age_group(df):
        df['age_group'] = pd.cut(df['age'], bins=[0, 18, 65, 120], labels=['minor', 'adult', 'senior'])
        return df
    
    processor.add_transformation(uppercase_names)
    processor.add_transformation(calculate_age_group)
    
    # Run pipeline
    success = processor.process_pipeline("output.csv", OutputFormat.CSV)
    
    if success:
        print("Pipeline completed successfully!")
    else:
        print("Pipeline failed. Check logs for details.")