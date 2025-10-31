import pandas as pd

# Path to your CSV file (use absolute path to be safe)
file_path = r"AP_IB_Course_2024.csv"

# Read the entire CSV file
df = pd.read_csv(file_path)

# Dictionary to store unique values for each column
unique_values = {}

# Collect unique values for each column
for col in df.columns:
    unique_values[col] = df[col].unique().tolist()

# Print ALL unique values for each column
for col, vals in unique_values.items():
    print(f"\n{'='*80}")
    print(f"Column: {col}")
    print(f"Number of unique values: {len(vals)}")
    print(f"Unique values:")
    print(vals)
