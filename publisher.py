"""Publisher COM wrapper for converting .pub files to PDF.

Exposes two entry points:
- `PublisherSession`: context manager that keeps a single Publisher instance
  alive across many conversions (use this for sequential batch jobs).
- `convert_pub_to_pdf`: standalone one-shot converter that spins up its own
  Publisher instance. Safe to call from a worker process.
"""

import os

import pythoncom
import win32com.client

PB_FIXED_FORMAT_PDF = 2  # pbFixedFormatTypePDF


def _start_publisher():
    """Launch a fresh Publisher COM instance with modal alerts suppressed."""
    publisher = win32com.client.DispatchEx("Publisher.Application")
    try:
        # Prevents "Update links?" and similar dialogs from hanging batch jobs
        publisher.DisplayAlerts = 0
    except Exception:
        # Not all Publisher versions expose this; non-fatal
        pass
    return publisher


class PublisherSession:
    """Context manager that keeps one Publisher instance alive for many files."""

    def __init__(self):
        self.publisher = None
        self._com_initialized = False

    def __enter__(self):
        pythoncom.CoInitialize()
        self._com_initialized = True
        self.publisher = _start_publisher()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.publisher is not None:
            try:
                self.publisher.Quit()
            except Exception:
                pass
            self.publisher = None
        if self._com_initialized:
            pythoncom.CoUninitialize()
            self._com_initialized = False
        return False

    def restart(self):
        """Quit and relaunch the underlying Publisher instance (releases memory)."""
        if self.publisher is not None:
            try:
                self.publisher.Quit()
            except Exception:
                pass
        self.publisher = _start_publisher()

    def convert(self, input_path, suffix="", overwrite=False):
        """Convert one .pub file to PDF. Returns output path, or None if skipped."""
        input_path = os.path.abspath(input_path)
        output_path = os.path.splitext(input_path)[0] + suffix + ".pdf"

        if not overwrite and os.path.exists(output_path):
            return None

        doc = None
        try:
            doc = self.publisher.Open(input_path)
            doc.ExportAsFixedFormat(PB_FIXED_FORMAT_PDF, output_path)
            return output_path
        finally:
            if doc is not None:
                try:
                    doc.Close()
                except Exception:
                    pass


def convert_pub_to_pdf(filename, suffix="_www", overwrite=True):
    """One-shot conversion — initializes COM and launches Publisher for a single file.

    Use this from worker processes or threads. For batch jobs in a single
    process, prefer `PublisherSession` to avoid repeatedly relaunching Publisher.
    """
    filename = os.path.abspath(filename)
    pdf_file = os.path.splitext(filename)[0] + suffix + ".pdf"

    if not overwrite and os.path.exists(pdf_file):
        return pdf_file

    pythoncom.CoInitialize()
    publisher = None
    doc = None
    try:
        publisher = _start_publisher()
        doc = publisher.Open(filename)
        doc.ExportAsFixedFormat(PB_FIXED_FORMAT_PDF, pdf_file)
        return pdf_file
    except Exception as e:
        raise RuntimeError(f"Failed to convert {filename}: {e}") from e
    finally:
        if doc is not None:
            try:
                doc.Close()
            except Exception:
                pass
        if publisher is not None:
            try:
                publisher.Quit()
            except Exception:
                pass
        pythoncom.CoUninitialize()