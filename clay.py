# INSERT API AT LINE 197
# python -m venv env
# source env/bin/activate  
# pip install -r requirements.txt
# python clay.py
import pandas as pd
import requests
import os
from datetime import datetime
import re
import time
from dotenv import load_dotenv

def validate_email(email, api_key):
    """
    Validates email using Abstract API
    Returns True if email is deliverable, False otherwise
    """
    print(f"\n🔍 Validating email: {email}")
    url = f"https://emailvalidation.abstractapi.com/v1/?api_key={api_key}&email={email}"
    try:
        print("  ⏳ Making API request...")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print("  ✓ API response received")
            # Check if the email is deliverable and has good quality
            is_deliverable = data.get('deliverability') == 'DELIVERABLE'
            quality_score = float(data.get('quality_score', 0))
            is_valid_format = data.get('is_valid_format', {}).get('value', False)
            
            result = is_deliverable and quality_score > 0.7 and is_valid_format
            print(f"  📊 Results: Deliverable: {is_deliverable}, Score: {quality_score}, Valid format: {is_valid_format}")
            return result
        
        elif response.status_code == 429:
            print(f"  ⚠️ Rate limit reached. Waiting 1 second before retrying...")
            time.sleep(1)  # Wait 1 second before next request
            return validate_email(email, api_key)  # Retry the request
        
        elif response.status_code == 422:
            print("  ❌ API quota reached. Please check your Abstract API plan.")
            return False
        
        print(f"  ❌ Unexpected status code: {response.status_code}")
        return False
    
    except Exception as e:
        print(f"  ❌ Error validating email {email}: {str(e)}")
        return False

def is_initials(name):
    """
    Check if name appears to be initials (e.g., "A.B." or "AB" or "A B")
    """
    if pd.isna(name):
        return False
    
    name = str(name).strip()
    # Check if name is just 1-3 letters possibly separated by spaces or dots
    return bool(re.match(r'^[A-Za-z](?:[. ]?[A-Za-z]){0,2}[.]?$', name))

def clean_first_name(name):
    """
    Cleans up first name according to specified rules
    Returns (cleaned_name, is_valid)
    """
    if pd.isna(name):
        return name, False
    
    # Convert to string in case it's not
    name = str(name).strip()
    
    # Check if it's initials
    if is_initials(name):
        return name, False
    
    # Check for any type of quotation pattern (e.g., Wen Jing 'David' or "David" or "David")
    # This will match any text between any type of quotes
    quote_match = re.search(r'[\'"]([^\'"]+)[\'"]|"([^"]+)"|\'([^\']+)\'', name)
    if quote_match:
        # Use the first non-None group (the quoted name)
        quoted_name = next((g for g in quote_match.groups() if g is not None), None)
        if quoted_name:
            return quoted_name.strip(), True
    
    # Split on spaces and take first part
    first_part = name.split()[0]
    
    # Capitalize first letter, lowercase rest
    return first_part.capitalize(), True

def process_csv(input_file, api_key):
    """
    Main function to process the CSV file
    """
    # Read the CSV
    print(f"\n📂 Reading file: {input_file}")
    df = pd.read_csv(input_file)
    print(f"📊 Found {len(df)} rows in the CSV")
    
    # Create report file name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_filename = os.path.splitext(os.path.basename(input_file))[0]
    report_file = f"{base_filename}_report_{timestamp}.txt"
    report_path = os.path.join(os.path.dirname(input_file), report_file)
    
    # Track invalid names
    invalid_names = []
    
    # Clean up first names and track invalid ones
    print("\n🧹 Cleaning up first names...")
    def clean_and_track(row):
        name, is_valid = clean_first_name(row['First Name'])
        if not is_valid:
            invalid_names.append({
                'Original Name': row['First Name'],
                'Email': row['Email']
            })
        return name
    
    df['First Name'] = df.apply(clean_and_track, axis=1)
    print("✓ First names cleaned")
    
    # Validate emails
    print("\n🔄 Starting email validation process...")
    print(f"⏳ This might take a while due to API rate limits (1 request per second)")
    total_emails = len(df)
    validated_count = 0
    
    def validate_with_progress(email):
        nonlocal validated_count
        validated_count += 1
        result = validate_email(email, api_key)
        print(f"Progress: {validated_count}/{total_emails} emails processed ({(validated_count/total_emails*100):.1f}%)")
        return result
    
    df['is_valid_email'] = df['Email'].apply(validate_with_progress)
    
    # Remove invalid emails
    valid_count = df['is_valid_email'].sum()
    print(f"\n✨ Email validation complete!")
    print(f"📊 Found {valid_count} valid emails out of {total_emails}")
    df = df[df['is_valid_email']].drop('is_valid_email', axis=1)
    
    # Generate output filename with timestamp
    output_file = f"{base_filename}_cleaned_{timestamp}.csv"
    output_path = os.path.join(os.path.dirname(input_file), output_file)
    
    # Save the cleaned data
    print("\n💾 Saving cleaned data...")
    df.to_csv(output_path, index=False)
    print(f"✓ Data saved to: {output_file}")
    
    # Write report file
    with open(report_path, 'w') as f:
        f.write(f"Report for {os.path.basename(input_file)}\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("Summary:\n")
        f.write(f"- Total rows processed: {total_emails}\n")
        f.write(f"- Valid emails: {valid_count}\n")
        f.write(f"- Invalid names found: {len(invalid_names)}\n\n")
        
        if invalid_names:
            f.write("Invalid Names Found:\n")
            f.write("-" * 50 + "\n")
            for entry in invalid_names:
                f.write(f"Original Name: {entry['Original Name']}\n")
                f.write(f"Email: {entry['Email']}\n")
                f.write("-" * 50 + "\n")
    
    print(f"📝 Report saved to: {report_file}")
    return output_file, report_file

def main():
    # Load environment variables
    load_dotenv()
    
    # Get API key from environment variable
    API_KEY = os.getenv('ABSTRACT_API_KEY')
    if not API_KEY:
        print("❌ Error: ABSTRACT_API_KEY not found in .env file")
        print("Please create a .env file with your API key like this:")
        print("ABSTRACT_API_KEY=your_api_key_here")
        return
    
    print("\n" + "="*50)
    print("CSV FILE INPUT INSTRUCTIONS")
    print("="*50)
    print("1. Your CSV files should be in your Downloads folder")
    print("2. You'll be prompted to enter each filename")
    print("\nExample filename: Computer-Vision-Researchers-People-V1-Default-View-export-1736733853732.csv")
    print("="*50 + "\n")
    
    # Ask for number of CSVs
    while True:
        try:
            num_files = int(input("How many CSV files do you want to process? "))
            if num_files > 0:
                break
            print("❌ Please enter a number greater than 0")
        except ValueError:
            print("❌ Please enter a valid number")
    
    # Collect all filenames
    files_to_process = []
    downloads_folder = os.path.expanduser("~/Downloads")
    
    for i in range(num_files):
        while True:
            csv_name = input(f"\n📄 Enter name of CSV file #{i+1}: ")
            if not csv_name.endswith('.csv'):
                csv_name += '.csv'
            
            input_file = os.path.join(downloads_folder, csv_name)
            
            if os.path.exists(input_file):
                files_to_process.append(input_file)
                print(f"✅ File #{i+1} added to queue: {csv_name}")
                break
            else:
                print(f"❌ Error: File not found: {csv_name}")
                print("Please check if:")
                print("- The file name is correct")
                print("- The file is in your Downloads folder")
    
    print(f"\n📋 Processing queue of {len(files_to_process)} files:")
    for i, file_path in enumerate(files_to_process, 1):
        print(f"\n{'='*50}")
        print(f"🔄 Processing file {i}/{len(files_to_process)}: {os.path.basename(file_path)}")
        print(f"{'='*50}")
        
        try:
            output_file, report_file = process_csv(file_path, API_KEY)
            print(f"\n✅ File {i} completed successfully!")
            print(f"📁 Output file: {output_file}")
            print(f"📝 Report file: {report_file}")
        except Exception as e:
            print(f"\n❌ Error processing file {i}: {str(e)}")
    
    print("\n🎉 All files processed!")
    print(f"📊 Summary: {len(files_to_process)} files processed")
    print("📁 Check your Downloads folder for the cleaned CSVs and reports")

if __name__ == "__main__":
    main()
