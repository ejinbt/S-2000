import pandas as pd
import sys

def scan_duplicates(csv_file, columns=None):
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # If specific columns are provided, check duplicates based on those columns
        if columns:
            duplicates = df[df.duplicated(subset=columns, keep=False)]
        else:
            # Check duplicates based on all columns
            duplicates = df[df.duplicated(keep=False)]
        
        # If duplicates are found, print them and save to a new CSV
        if not duplicates.empty:
            print(f"Found {len(duplicates)} duplicate rows:")
            print(duplicates)
            
            # Save duplicates to a new CSV file
            output_file = csv_file.replace('.csv', '_duplicates.csv')
            duplicates.to_csv(output_file, index=False)
            print(f"\nDuplicates saved to {output_file}")
        else:
            print("No duplicates found.")
            
        return duplicates
    
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python scan_duplicates.py <csv_file> [column1,column2,...]")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    columns = sys.argv[2].split(',') if len(sys.argv) > 2 else None
    scan_duplicates(csv_file, columns)