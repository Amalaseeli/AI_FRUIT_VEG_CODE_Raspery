import os
import sys

APP_NAME = "SCSFC"

def resource_path(rel_path:str) -> str:
    """
    Get the absolute path to a resource, works for dev and for PyInstaller.
    This is for read-only files packaged with the application.

    When you package a Python app with PyInstaller, 
    your data files (images, templates, etc.) are bundled and extracted to a temporary folder at runtime.
    Their location isn’t the same as during normal development.
    This helper returns the correct absolute path to a resource in both cases.
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return(os.path.join(sys._MEIPASS, rel_path))
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), rel_path)

def get_data_dir() -> str:
    """
    Returns a writable directory for user data (e.g., roi.yaml).
    For a packaged app, this will be in the user's AppData folder.
    For development, it will be the script's directory.
    """
    if getattr(sys, "frozen", False):
        if sys.platform == "win32":
            data_dir = os.path.join(os.environ["APPDATA"], APP_NAME)
        else:
            data_dir = os.path.join(os.path.expanduser("~"), ".config", APP_NAME)
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    else:
        return os.path.dirname(os.path.abspath(__file__))


MODEL_PATH = resource_path(os.path.join("models", "AI_Fruit_veg_code_float16.tflite"))

classNames = ['AI001', 'AI002', 'AI003', 'AI004', 'AI005', 'AI006', 'AI007', 'AI008', 'AI009', 'AI010']

DATA_DIR = get_data_dir()
ROI_PATH = os.path.join(DATA_DIR, "roi.txt")

