"""
Find all ATOMs for YCHSWAGGA program and identify those not matched to episodes.
"""
import os
import pandas as pd
from azure.data.tables import TableServiceClient

# Load connection string from local.settings.json or environment
import json
with open(r"C:\Users\aftab.jalal\dev\NADATools_AzFunc-program-filtering\local.settings.json") as f:
    settings = json.load(f)
    conn_str = settings["Values"]["AZURE_STORAGE_CONNECTION_STRING"]

# Query all ATOMs for YCHSWAGGA
table_service = TableServiceClient.from_connection_string(conn_str)
table_client = table_service.get_table_client("ATOM")

print("Querying all ATOMs for program YCHSWAGGA...")
query_filter = "Program eq 'YCHSWAGGA'"
entities = list(table_client.query_entities(query_filter, select=["PartitionKey", "RowKey", "AssessmentDate", "Program", "SLK"]))

print(f"Found {len(entities)} ATOMs for YCHSWAGGA")

# Convert to DataFrame
atoms_df = pd.DataFrame(entities)
atoms_df = atoms_df.rename(columns={"PartitionKey": "SLK_PK", "RowKey": "RowKey"})

# Load matched assessments from blob storage output
matched_file = r"C:\Users\aftab.jalal\Downloads\forstxt_20251001-20251231_YCHSWAGGA_matched.csv"
if os.path.exists(matched_file):
    matched_df = pd.read_csv(matched_file, dtype=str)
    matched_rowkeys = set(matched_df["RowKey"].unique()) if "RowKey" in matched_df.columns else set()
    print(f"Found {len(matched_rowkeys)} matched ATOMs in output file")
else:
    print(f"Matched file not found at {matched_file}")
    print("Trying alternative location...")
    # Try to find the file
    matched_rowkeys = set()

# Find unmatched ATOMs
atoms_df["matched"] = atoms_df["RowKey"].isin(matched_rowkeys)
unmatched = atoms_df[~atoms_df["matched"]]

print(f"\n=== Summary ===")
print(f"Total ATOMs for YCHSWAGGA: {len(atoms_df)}")
print(f"Matched ATOMs: {len(atoms_df[atoms_df['matched']])}")
print(f"Unmatched ATOMs: {len(unmatched)}")

# Show unmatched ATOMs
if len(unmatched) > 0:
    print(f"\n=== Unmatched ATOMs ({len(unmatched)}) ===")
    print(unmatched[["SLK_PK", "RowKey", "AssessmentDate"]].to_string(index=False))

    # Save to CSV
    output_file = r"C:\Users\aftab.jalal\Downloads\YCHSWAGGA_unmatched_atoms.csv"
    unmatched.to_csv(output_file, index=False)
    print(f"\nSaved unmatched ATOMs to: {output_file}")
