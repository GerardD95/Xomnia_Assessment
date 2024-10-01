from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
import re

@udf(FloatType())
def clean_numerical_col(val: str) -> float:
    try:
        # Remove all invalid characters except the dot
        val = re.sub(r'[^0-9.]', '', val)
        
        # Keep only the first dot and remove the rest
        if val.count('.') > 1:
            parts = val.split('.')
            val = parts[0] + '.' + ''.join(parts[1:])
        return float(val) if val else None
    except ValueError:
        return None

@udf(StringType())
def clean_alphabetical_col(val: str) -> str:
    # Only keep alphabetical characters
    val = re.sub(r'[^a-zA-Z]', '', val)
    return val if val else None