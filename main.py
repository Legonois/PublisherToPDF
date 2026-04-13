"""CLI entry point: recursively convert .pub files under a folder to PDF.

Uses a background thread pool to pre-download (hydrate) files from Google
Drive / OneDrive placeholders while Publisher converts already-downloaded
files sequentially. This overlaps network I/O with Publisher CPU time.
"""

import argparse
import os
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

from hydrate import hydrate
from publisher import PublisherSession

RESTART_EVERY = 50           # Relaunch Publisher every N files to curb memory leaks
DEFAULT_PREFETCH_WORKERS = 4  # Parallel download threads
DEFAULT_QUEUE_DEPTH = 8       # Max hydrated files waiting for Publisher


# Sentinel pushed onto the queue to signal "no more items"
_DONE = object()


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
    parser.add_argument(
        "--prefetch-workers",
        type=int,
        default=DEFAULT_PREFETCH_WORKERS,
        help=f"Number of parallel download threads (default: {DEFAULT_PREFETCH_WORKERS})",
    )
    parser.add_argument(
        "--queue-depth",
        type=int,
        default=DEFAULT_QUEUE_DEPTH,
        help=f"Max hydrated files buffered ahead of Publisher (default: {DEFAULT_QUEUE_DEPTH})",
    )
    parser.add_argument(
        "--no-prefetch",
        action="store_true",
        help="Disable background download; process strictly sequentially",
    )
    return parser.parse_args()


def needs_conversion(input_path, suffix, overwrite):
    """Return True if this file should be converted (not already done)."""
    if overwrite:
        return True
    output_path = os.path.splitext(input_path)[0] + suffix + ".pdf"
    return not os.path.exists(output_path)


def prefetch_worker(input_paths, ready_queue, stop_event, prefetch_workers):
    """Hydrate files in parallel, push (path, error) tuples to ready_queue."""
    def _hydrate_one(path):
        try:
            hydrate(path)
            return (path, None)
        except Exception as e:
            return (path, e)

    with ThreadPoolExecutor(max_workers=prefetch_workers) as pool:
        futures = [pool.submit(_hydrate_one, p) for p in input_paths]
        for fut in futures:
            if stop_event.is_set():
                break
            result = fut.result()
            # Block on full queue — this is the backpressure that keeps us
            # from hydrating the entire drive ahead of Publisher.
            while not stop_event.is_set():
                try:
                    ready_queue.put(result, timeout=0.5)
                    break
                except queue.Full:
                    continue

    ready_queue.put(_DONE)


def main():
    args = parse_args()

    folder_path = os.path.abspath(args.folder)
    if not os.path.isdir(folder_path):
        print(f"Not a directory: {folder_path}")
        sys.exit(1)

    all_pub_files = find_pub_files(folder_path)
    if not all_pub_files:
        print("No .pub files found.")
        sys.exit(0)

    # Filter out already-converted files up front so we don't waste downloads
    pub_files = [p for p in all_pub_files if needs_conversion(p, args.suffix, args.overwrite)]
    pre_skipped = len(all_pub_files) - len(pub_files)

    if not pub_files:
        print(f"All {len(all_pub_files)} files already converted. Nothing to do.")
        sys.exit(0)

    print(
        f"Found {len(all_pub_files)} files "
        f"({pre_skipped} already converted, {len(pub_files)} to process)."
    )

    errors = []

    if args.no_prefetch:
        _run_sequential(pub_files, args, errors)
        converted = len(pub_files) - len(errors)
    else:
        print(
            f"Starting with {args.prefetch_workers} download workers, "
            f"queue depth {args.queue_depth}..."
        )
        converted = _run_with_prefetch(pub_files, args, errors)

    print("\n--- Processing Complete ---")
    print(f"Converted: {converted}")
    if pre_skipped:
        print(f"Skipped (PDF already existed): {pre_skipped}")
    if errors:
        print(f"Errors: {len(errors)}")
        for err in errors:
            print(f"  {err}")
    else:
        print("No errors.")


def _run_sequential(pub_files, args, errors):
    with PublisherSession() as session:
        for i, input_path in enumerate(tqdm(pub_files), start=1):
            try:
                hydrate(input_path)
                session.convert(input_path, suffix=args.suffix, overwrite=args.overwrite)
            except Exception as e:
                errors.append(f"{input_path}: {e}")
            if i % RESTART_EVERY == 0 and i < len(pub_files):
                session.restart()


def _run_with_prefetch(pub_files, args, errors):
    ready_queue = queue.Queue(maxsize=args.queue_depth)
    stop_event = threading.Event()

    producer = threading.Thread(
        target=prefetch_worker,
        args=(pub_files, ready_queue, stop_event, args.prefetch_workers),
        daemon=True,
    )
    producer.start()

    converted = 0
    processed = 0

    try:
        with PublisherSession() as session:
            with tqdm(total=len(pub_files)) as pbar:
                while True:
                    item = ready_queue.get()
                    if item is _DONE:
                        break

                    input_path, hydrate_err = item
                    try:
                        if hydrate_err is not None:
                            raise hydrate_err
                        session.convert(
                            input_path,
                            suffix=args.suffix,
                            overwrite=args.overwrite,
                        )
                        converted += 1
                    except Exception as e:
                        errors.append(f"{input_path}: {e}")

                    processed += 1
                    pbar.update(1)

                    if processed % RESTART_EVERY == 0 and processed < len(pub_files):
                        session.restart()
    finally:
        stop_event.set()
        producer.join(timeout=5)

    return converted


if __name__ == "__main__":
    main()