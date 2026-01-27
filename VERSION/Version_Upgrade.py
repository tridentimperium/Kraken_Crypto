import os
import re
import shutil

def is_text_file(filename):
    """Determine if a file is a text file based on its extension."""
    text_extensions = ['.py', '.env', '.json', '.txt']  # Add more extensions if needed
    return any(filename.lower().endswith(ext) for ext in text_extensions)

def update_file_content(file_path, from_version, to_version):
    """Replace the old version with the new version in the file content (in place)."""
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"File does not exist: {file_path}. Skipping.")
            return False

        # Read the original file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Pattern 1: Match Crypto_{from_version}_ (with trailing underscore)
        pattern_with_underscore = r'Crypto_' + re.escape(from_version) + r'_'
        occurrences_with_underscore = len(re.findall(pattern_with_underscore, content))
        print(f"Found {occurrences_with_underscore} occurrences of 'Crypto_{from_version}_' in {file_path}")

        # Pattern 2: Match Crypto_{from_version} (without trailing underscore)
        pattern_without_underscore = r'Crypto_' + re.escape(from_version) + r'\b'
        occurrences_without_underscore = len(re.findall(pattern_without_underscore, content))
        print(f"Found {occurrences_without_underscore} occurrences of 'Crypto_{from_version}' (without trailing underscore) in {file_path}")

        # Total occurrences
        total_occurrences = occurrences_with_underscore + occurrences_without_underscore
        print(f"Total occurrences of 'Crypto_{from_version}' (with or without underscore): {total_occurrences}")

        # Replace both patterns
        updated_content = content
        updated_content = re.sub(pattern_with_underscore, f'Crypto_{to_version}_', updated_content)
        updated_content = re.sub(pattern_without_underscore, f'Crypto_{to_version}', updated_content)

        # Verify replacements
        remaining_with_underscore = len(re.findall(pattern_with_underscore, updated_content))
        remaining_without_underscore = len(re.findall(pattern_without_underscore, updated_content))
        total_remaining = remaining_with_underscore + remaining_without_underscore
        if total_remaining > 0:
            print(f"Warning: {total_remaining} occurrences of 'Crypto_{from_version}' remain in {file_path} after replacement:")
            updated_lines = updated_content.splitlines()
            for line_num, line in enumerate(updated_lines, 1):
                if re.search(pattern_with_underscore, line) or re.search(pattern_without_underscore, line):
                    print(f"Line {line_num}: {line.strip()}")

        # Write the updated content back to the file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)

        print(f"Successfully updated {file_path} with version from {from_version} to {to_version}")
        return True

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return False

def get_files_to_process(from_version, to_version):
    """Return a list of (old_relative_path, new_relative_path) tuples for hardcoded files."""
    # Hardcoded list of files with version placeholder (relative paths from project root)
    file_templates = [
        "CONFIG/CLOUDAPI/Crypto_{version}_cloudapi.env",
        "CONFIG/SQLSERVER/Crypto_{version}_sqlserver_local.env",
        "CONFIG/SQLSERVER/Crypto_{version}_sqlserver_remote.env",
        "CONFIG/ZZ_PARAMETERS/Crypto_{version}_parameters.json",
        "CONFIG/ZZ_VARIABLES/Crypto_{version}_variables.json",
        "EXECUTION/Crypto_{version}_DEV_00_00_Batch.py",
        "EXECUTION/Crypto_{version}_DEV_00_00_Log.py",
        "EXECUTION/Crypto_{version}_DEV_01_01_Fetch_Data_Local.py",
        "EXECUTION/Crypto_{version}_DEV_01_01_Fetch_Data.py",
        "EXECUTION/Crypto_{version}_DEV_01_02_Analysis.py",
        "EXECUTION/Crypto_{version}_DEV_01_03_Analysis_Graph.py",
        "EXECUTION/Crypto_{version}_DEV_01_04_Backtest.py",
        "EXECUTION/Crypto_{version}_DEV_01_05_Backtest_Graph.py",
        "EXECUTION/Crypto_{version}_DEV_01_06_Entry_Exit_Order.py",
        "EXECUTION/Crypto_{version}_DEV_01_07_Results_Analysis.py",
        "EXECUTION/Crypto_{version}_DEV_01_08_Portfolio_Balance.py",
        "EXECUTION/Crypto_{version}_DEV_01_09_Portfolio_Summary.py",
        "EXECUTION/Crypto_{version}_DEV_01_10_Portfolio_Graph.py"
    ]

    # Replace the version placeholder in the file paths with the from_version and to_version
    files_to_process = []
    for file_template in file_templates:
        if from_version.lower() == "master":
            directory = os.path.dirname(file_template)
            old_relative_path = os.path.join(directory, "Master", os.path.basename(file_template.format(version=from_version)))
        else:
            old_relative_path = file_template.format(version=from_version)
        new_relative_path = file_template.format(version=to_version)
        files_to_process.append((old_relative_path, new_relative_path))

    return files_to_process

def update_project_versions():
    # Step 1: Prompt for the version to change from
    from_version = input("Enter the version number to change from (e.g., 001 or Master): ").strip()

    # Validate the "from" version
    if not (from_version.isdigit() or from_version.lower() == "master"):
        print("Invalid 'from' version. Please enter a numeric value (e.g., 001) or 'Master'.")
        return

    # Step 2: Prompt for the version to change to
    to_version = input("Enter the version number to change to (e.g., 002): ").strip()

    # Validate the "to" version
    if not to_version.isdigit():
        print("Invalid 'to' version. Please enter a numeric value (e.g., 002).")
        return

    # Normalize 'Master' to consistent casing for patterns
    from_version_for_pattern = from_version if from_version.isdigit() else "Master"

    print(f"Will change version from {from_version_for_pattern} to {to_version}")

    # Get the parent directory (main KRAKEN_CRYPTO folder)
    script_path = os.path.abspath(__file__)
    version_dir = os.path.dirname(script_path)
    parent_dir = os.path.dirname(version_dir)

    # Define the old and new versioned folders
    old_root = os.path.join(parent_dir, f"Kraken_Crypto_{from_version_for_pattern}")
    new_root = os.path.join(parent_dir, f"Kraken_Crypto_{to_version}")

    # Check if the old folder exists
    if not os.path.exists(old_root):
        print(f"Source folder does not exist: {old_root}.")
        return

    # Check if the new folder already exists
    if os.path.exists(new_root):
        print(f"Failed: The new folder '{new_root}' already exists. Please delete or archive it before upgrading.")
        return

    # Step 3: Copy the entire old versioned folder to the new versioned folder
    shutil.copytree(old_root, new_root)
    print(f"Successfully copied folder from '{old_root}' to '{new_root}'")

    # Step 4: Get the list of files to process
    files_to_process = get_files_to_process(from_version_for_pattern, to_version)

    if not files_to_process:
        print(f"No files found containing version {from_version_for_pattern} in the hardcoded list.")
        return

    print(f"Found {len(files_to_process)} files to process:")

    # Step 5: Process all files
    processed_files = 0
    for old_relative_path, new_relative_path in files_to_process:
        old_path = os.path.join(new_root, old_relative_path)
        new_path = os.path.join(new_root, new_relative_path)

        if not os.path.exists(old_path):
            print(f"Source file does not exist in new folder: {old_path}. Skipping.")
            continue

        print(f" - {old_relative_path}")

        if old_relative_path != new_relative_path:
            if os.path.exists(new_path):
                print(f"Target file already exists: {new_path}. Skipping.")
                continue
            os.rename(old_path, new_path)

        # Skip content update for specific files
        if "sqlserver_local.env" in new_relative_path or "sqlserver_remote.env" in new_relative_path:
            print(f"Skipping content update for {new_relative_path}")
            processed_files += 1
            continue

        print(f"\nProcessing: {old_relative_path} -> {new_relative_path}")
        if update_file_content(new_path, from_version_for_pattern, to_version):
            processed_files += 1

    print(f"\nCompleted updating {processed_files} out of {len(files_to_process)} files from version {from_version_for_pattern} to {to_version}.")

if __name__ == "__main__":
    update_project_versions()