## Input Format
The script expects a CSV file containing email addresses exported from Clay. The input file should have at least one column containing email addresses.

## Output Format
The script generates a cleaned CSV file with:
- Standardized email formats
- Removed duplicates
- Validated email addresses
- Additional metadata (if applicable)

## Requirements
- Python 3.6+
- Required Python packages:
  - pandas
  - email-validator

  ```bash
python -m venv env
source env/bin/activate  
pip install -r requirements.txt
  ```
