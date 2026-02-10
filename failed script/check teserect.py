import os
import cv2
import pytesseract
import pandas as pd
import numpy as np

# 1. Tesseract Path (Update for your Windows setup)
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def preprocess_for_300dpi(img_path):
    img = cv2.imread(img_path)
    if img is None:
        return None, None
        
    # Convert to Grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # 2. DIGITAL UPSCALING (Simulating 300 DPI)
    # We increase the size by 3x to ensure small numbers are large enough for OCR
    height, width = gray.shape
    upscaled = cv2.resize(gray, (width * 3, height * 3), interpolation=cv2.INTER_CUBIC)
    
    # 3. CONTRAST ENHANCEMENT
    # This makes the handwriting "pop" against the ledger paper
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
    contrast = clahe.apply(upscaled)
    
    # 4. BINARIZATION
    thresh = cv2.adaptiveThreshold(contrast, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                  cv2.THRESH_BINARY_INV, 21, 10)
    
    # 5. HEAL STROKES (Closing)
    # This fills small gaps in handwritten numbers (like an 8 that isn't fully closed)
    kernel = np.ones((3,3), np.uint8)
    closing = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
    
    return closing

# --- Main Logic ---
current_dir = os.getcwd()
files = [f for f in os.listdir(current_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg')) 
         and "DEBUG" not in f and "Result" not in f]

for f in files:
    processed_img = preprocess_for_300dpi(os.path.join(current_dir, f))
    if processed_img is None: continue
    
    # Save a debug image to check if the upscale looks clean
    cv2.imwrite(f"DEBUG_300DPI_{f}", processed_img)

    # OCR Configuration
    # We restrict to numbers and time chars to stop the "rubbish" output
    config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789.:/ '

    h, w = processed_img.shape
    mid = w // 2
    day_area = processed_img[:, :mid]
    night_area = processed_img[:, mid:]

    day_text = pytesseract.image_to_string(day_area, config=config)
    night_text = pytesseract.image_to_string(night_area, config=config)

    # Parse and Export
    day_rows = [line.split() for line in day_text.split('\n') if len(line.split()) > 1]
    night_rows = [line.split() for line in night_text.split('\n') if len(line.split()) > 1]

    # Use Pandas to align and save
    df_day = pd.DataFrame(day_rows)
    df_night = pd.DataFrame(night_rows)
    
    with pd.ExcelWriter(f"DPI_Cleaned_{f}.xlsx", engine='xlsxwriter') as writer:
        df_day.to_excel(writer, sheet_name='Day_Shift', index=False, header=False)
        df_night.to_excel(writer, sheet_name='Night_Shift', index=False, header=False)
    
    print(f"Processed {f}: Found {len(day_rows)} rows in Day and {len(night_rows)} in Night.")

print("Check the 'DEBUG_300DPI' files to see the new image quality.")