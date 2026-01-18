import os
import win32com.client
import pythoncom  # <--- You must install this (pip install pywin32)

def convertPubToPDF(filename, suffix="_www"):
    # 1. Initialize COM context for this specific thread
    pythoncom.CoInitialize()
    
    publisher = None
    try:
        # Using DispatchEx ensures a fresh instance is usually safer for threads
        publisher = win32com.client.Dispatch("Publisher.Application")
        
        # Optional: Hide the window to prevent UI popping up constantly
        # publisher.ActiveWindow.Visible = False 
        
        doc = publisher.Open(filename)
        
        pdf_file = os.path.splitext(filename)[0] + suffix + ".pdf"
        
        # 2 represents pbFixedFormatTypePDF
        doc.ExportAsFixedFormat(2, pdf_file) 
        
        doc.Close()
        
    except Exception as e:
        # Re-raise the error so the main script counts it as a failure
        raise e
        
    finally:
        # 2. Always quit Publisher and uninitialize COM, even if errors occur
        if publisher:
            publisher.Quit()
        pythoncom.CoUninitialize()