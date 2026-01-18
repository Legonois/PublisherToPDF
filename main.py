import os
import argparse
import win32com.client
from tqdm import tqdm
# import pythoncom

# 1. Setup Arguments
parser = argparse.ArgumentParser()
parser.add_argument("folder", help="Path to folder containing pub files")
args = parser.parse_args()

# 2. Get list of files (RECURSIVELY)
folder_path = os.path.abspath(args.folder)
pub_files = []

# os.walk automatically goes through every subfolder
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith(".pub"):
            # We store the full path immediately
            pub_files.append(os.path.join(root, file))

if not pub_files:
    print("No .pub files found.")
    exit()

print(f"Found {len(pub_files)} files. Starting SEQUENTIAL conversion...")

# 3. Open Publisher ONCE (Outside the loop)
try:
    # pythoncom.CoInitialize()
    publisher = win32com.client.Dispatch("Publisher.Application")
    # CRITICAL: This stops popups like "Update Links?" from blocking the script
    # publisher.DisplayAlerts = 0 
except Exception as e:
    print(e)
    print("Failed to open Publisher")
    exit()

# 4. Loop through files one by one
errors = []

for file_name in tqdm(pub_files):
    input_path = os.path.join(folder_path, file_name)
    output_path = os.path.splitext(input_path)[0] + ".pdf"
    
    try:
        # Open the document
        doc = publisher.Open(input_path)
        
        # Export (2 = pbFixedFormatTypePDF)
        doc.ExportAsFixedFormat(2, output_path)
        
        # Close the document strictly
        doc.Close()
        
    except Exception as e:
        errors.append(f"{file_name}: {e}")

# 5. Clean up
publisher.Quit()

print("\n--- Processing Complete ---")
if errors:
    print(f"Completed with {len(errors)} errors:")
    for err in errors:
        print(err)
else:
    print("All files converted successfully.")
