import pandas as pd
import os
from datetime import datetime

def dedupe_csv(master_file, input_file):
    """
    Dedupes the input CSV against the master CSV.
    Uses 'LinkedIn URL' in master file and 'LinkedIn Profile' in input file.
    Returns a new CSV with only the unique LinkedIn profiles not found in the master CSV.
    """
    # Read the master CSV
    print(f"\n📂 Reading master file: {master_file}")
    try:
        master_df = pd.read_csv(master_file)
        print(f"📊 Found {len(master_df)} rows in the master CSV")
    except Exception as e:
        print(f"❌ Error reading master file: {str(e)}")
        return None
    
    # Read the input CSV
    print(f"\n📂 Reading input file: {input_file}")
    try:
        input_df = pd.read_csv(input_file)
        print(f"📊 Found {len(input_df)} rows in the input CSV")
    except Exception as e:
        print(f"❌ Error reading input file: {str(e)}")
        return None
    
    # Check if required columns exist in both files
    if 'LinkedIn URL' not in master_df.columns:
        print("❌ Error: 'LinkedIn URL' column not found in master CSV")
        print(f"Available columns: {', '.join(master_df.columns)}")
        return None
    
    if 'LinkedIn Profile' not in input_df.columns:
        print("❌ Error: 'LinkedIn Profile' column not found in input CSV")
        print(f"Available columns: {', '.join(input_df.columns)}")
        return None
    
    # Get all LinkedIn URLs from master file
    master_urls = set(master_df['LinkedIn URL'].dropna().str.strip().tolist())
    print(f"🔗 Found {len(master_urls)} unique LinkedIn URLs in master file")
    
    # Filter input file to only include rows with LinkedIn Profiles not in master file
    print("\n🔍 Finding unique LinkedIn Profiles...")
    
    # Count before filtering
    total_rows = len(input_df)
    
    # Create a mask for rows with LinkedIn Profiles not in master file
    # Also filter out empty/NaN LinkedIn Profiles
    input_df['LinkedIn Profile'] = input_df['LinkedIn Profile'].fillna('').str.strip()
    unique_mask = ~input_df['LinkedIn Profile'].isin(master_urls) & (input_df['LinkedIn Profile'] != '')
    
    # Apply the mask
    unique_df = input_df[unique_mask]
    
    # Count after filtering
    unique_rows = len(unique_df)
    duplicate_rows = total_rows - unique_rows
    
    print(f"✅ Found {unique_rows} unique LinkedIn Profiles not in master file")
    print(f"🔄 Removed {duplicate_rows} duplicate LinkedIn Profiles")
    
    # Generate output filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    output_file = f"{base_filename}_unique_{timestamp}.csv"
    output_path = os.path.join(os.path.dirname(input_file), output_file)
    
    # Save the unique data
    print("\n💾 Saving unique data...")
    unique_df.to_csv(output_path, index=False)
    print(f"✓ Data saved to: {output_file}")
    
    return output_file

def main():
    print("\n" + "="*50)
    print("CSV DEDUPLICATION TOOL")
    print("="*50)
    print("This tool will dedupe a CSV file against a master list based on LinkedIn profiles.")
    print("Both files should be in your Downloads folder.")
    print("="*50 + "\n")
    
    downloads_folder = os.path.expanduser("~/Downloads")
    
    # Master file is fixed
    master_file = os.path.join(downloads_folder, "Export (inclusive of people emailed) - People.csv")
    
    if not os.path.exists(master_file):
        print(f"❌ Error: Master file not found at {master_file}")
        print("Please make sure the master file is in your Downloads folder with the correct name.")
        return
    
    # Ask for the input file
    while True:
        input_filename = input("\n📄 Enter name of CSV file to dedupe: ")
        if not input_filename.endswith('.csv'):
            input_filename += '.csv'
        
        input_file = os.path.join(downloads_folder, input_filename)
        
        if os.path.exists(input_file):
            print(f"✅ File found: {input_filename}")
            break
        else:
            print(f"❌ Error: File not found: {input_filename}")
            print("Please check if:")
            print("- The file name is correct")
            print("- The file is in your Downloads folder")
    
    # Process the files
    try:
        output_file = dedupe_csv(master_file, input_file)
        if output_file:
            print("\n🎉 Deduplication completed successfully!")
            print(f"📁 Output file: {output_file}")
    except Exception as e:
        print(f"\n❌ Error during deduplication: {str(e)}")

if __name__ == "__main__":
    main()
