"""CLI entry point: recursively convert .pub files under a folder to PDF."""

import argparse
import os
import sys

from tqdm import tqdm

from publisher import PublisherSession

RESTART_EVERY = 50  # Relaunch Publisher every N files to curb memory leaks


def find_pub_files(folder_path):
    """Return absolute paths to every .pub file under folder_path (recursive)."""
    pub_files = []
    for root, _dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".pub"):
                pub_files.append(os.path.abspath(os.path.join(root, file)))
    return pub_files


def parse_args():
    parser = argparse.ArgumentParser(description="Recursively convert .pub files to PDF.")
    parser.add_argument("folder", help="Path to folder containing .pub files")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing PDFs (default: skip files whose PDF already exists)",
    )
    parser.add_argument(
        "--suffix",
        default="",
        help='Suffix to append to output PDF filenames (e.g. "_www")',
    )
    return parser.parse_args()


def main():
    args = parse_args()

    folder_path = os.path.abspath(args.folder)
    if not os.path.isdir(folder_path):
        print(f"Not a directory: {folder_path}")
        sys.exit(1)

    pub_files = find_pub_files(folder_path)
    if not pub_files:
        print("No .pub files found.")
        sys.exit(0)

    print(f"Found {len(pub_files)} files. Starting sequential conversion...")

    errors = []
    skipped = []
    converted = 0

    with PublisherSession() as session:
        for i, input_path in enumerate(tqdm(pub_files), start=1):
            try:
                result = session.convert(
                    input_path,
                    suffix=args.suffix,
                    overwrite=args.overwrite,
                )
                if result is None:
                    skipped.append(input_path)
                else:
                    converted += 1
            except Exception as e:
                errors.append(f"{input_path}: {e}")

            # Periodically restart Publisher to release accumulated memory
            if i % RESTART_EVERY == 0 and i < len(pub_files):
                session.restart()

    print("\n--- Processing Complete ---")
    print(f"Converted: {converted}")
    if skipped:
        print(f"Skipped (PDF already existed): {len(skipped)}")
    if errors:
        print(f"Errors: {len(errors)}")
        for err in errors:
            print(f"  {err}")
    else:
        print("No errors.")


if __name__ == "__main__":
    main()