#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Thai OCR with PDF Support
รองรับทั้ง PDF และไฟล์ภาพ
"""

import cv2
import numpy as np
import pytesseract
from PIL import Image
import re
import os

# สำหรับ PDF
try:
    import PyPDF2
    import fitz  # PyMuPDF
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    print("⚠️  PDF libraries not installed. Install with:")
    print("   pip install PyPDF2 PyMuPDF")


class EnhancedThaiDocumentOCR:
    def __init__(self):
        self.thai_digits = {
            '0': '๐', '1': '๑', '2': '๒', '3': '๓', '4': '๔',
            '5': '๕', '6': '๖', '7': '๗', '8': '๘', '9': '๙'
        }

    # ============================================================
    # 🔹 ตรวจสอบ PDF มี text layer หรือไม่
    # ============================================================
    def check_pdf_has_text(self, pdf_path):
        if not PDF_SUPPORT:
            return False

        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                pages_to_check = min(3, len(pdf_reader.pages))
                for i in range(pages_to_check):
                    page = pdf_reader.pages[i]
                    text = page.extract_text()
                    thai_chars = len(re.findall(r'[ก-ฮะ-์]', text))
                    if thai_chars > 50:
                        return True
            return False
        except Exception as e:
            print(f"Error checking PDF: {e}")
            return False

    # ============================================================
    # 🔹 Extract text จาก PDF ที่มี text layer
    # ============================================================
    def extract_text_from_pdf(self, pdf_path):
        if not PDF_SUPPORT:
            raise ImportError("PyPDF2 not installed")

        print("✓ PDF has text layer - extracting directly...")
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                all_text = []
                for i, page in enumerate(pdf_reader.pages):
                    print(f"  Page {i+1}/{len(pdf_reader.pages)}")
                    text = page.extract_text()
                    all_text.append(text)
                return '\n\n'.join(all_text)
        except Exception as e:
            print(f"✗ Error extracting text: {e}")
            return None

    # ============================================================
    # 🔹 แปลง PDF เป็นภาพ (กรณีไม่มี text layer)
    # ============================================================
    def pdf_to_images(self, pdf_path, output_folder='temp_pages', dpi=300):
        if not PDF_SUPPORT:
            raise ImportError("PyMuPDF not installed")

        print(f"Converting PDF to images (DPI={dpi})...")
        os.makedirs(output_folder, exist_ok=True)

        try:
            doc = fitz.open(pdf_path)
            image_paths = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                zoom = dpi / 72
                mat = fitz.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                output_path = os.path.join(output_folder, f'page_{page_num+1}.png')
                pix.save(output_path)
                image_paths.append(output_path)
                print(f"  ✓ Page {page_num+1}/{len(doc)} saved: {output_path}")
            doc.close()
            return image_paths
        except Exception as e:
            print(f"✗ Error converting PDF: {e}")
            return []

    # ============================================================
    # 🔹 Preprocess ภาพคุณภาพต่ำ
    # ============================================================
    def preprocess_for_low_quality(self, image_path):
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Cannot read image: {image_path}")

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        height, width = gray.shape
        if height < 3500:
            scale = 3500 / height
            gray = cv2.resize(gray, (int(width * scale), int(height * scale)), interpolation=cv2.INTER_LANCZOS4)

        bilateral = cv2.bilateralFilter(gray, 9, 75, 75)
        denoised = cv2.fastNlMeansDenoising(bilateral, None, 20, 7, 21)
        kernel_sharpen = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
        sharpened = cv2.filter2D(denoised, -1, kernel_sharpen)
        clahe = cv2.createCLAHE(clipLimit=4.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(sharpened)
        gaussian = cv2.GaussianBlur(enhanced, (0, 0), 2.0)
        unsharp = cv2.addWeighted(enhanced, 1.5, gaussian, -0.5, 0)
        binary = cv2.adaptiveThreshold(unsharp, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY, 21, 2)
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2, 2))
        binary = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
        binary = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel)
        return binary

    # ============================================================
    # 🔹 Preprocess สำหรับภาพ PDF คุณภาพสูง
    # ============================================================
    def preprocess_for_high_quality(self, image_path):
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Cannot read image: {image_path}")

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        height, width = gray.shape
        if height < 2500:
            scale = 2500 / height
            gray = cv2.resize(gray, (int(width * scale), int(height * scale)), interpolation=cv2.INTER_CUBIC)
        denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
        clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
        enhanced = clahe.apply(denoised)
        binary = cv2.adaptiveThreshold(enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY, 15, 4)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 1))
        binary = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
        return binary

    # ============================================================
    # 🔹 OCR หลาย config แล้วเลือกผลดีที่สุด
    # ============================================================
    def ocr_with_multiple_configs(self, image):
        configs = [
            '--oem 1 --psm 6 -l tha',
            '--oem 1 --psm 4 -l tha',
            '--oem 1 --psm 3 -l tha',
            '--oem 3 --psm 6 -l tha',
            '--oem 1 --psm 11 -l tha',
        ]

        results = []
        for config in configs:
            try:
                text = pytesseract.image_to_string(image, config=config)
                data = pytesseract.image_to_data(image, config=config, output_type=pytesseract.Output.DICT)
                confs = [int(c) for c in data['conf'] if str(c) != '-1']
                avg_conf = sum(confs) / len(confs) if confs else 0
                thai_chars = len(re.findall(r'[ก-ฮะ-์]', text))
                results.append({'text': text, 'confidence': avg_conf, 'thai_chars': thai_chars, 'config': config})
            except Exception:
                pass

        if results:
            return max(results, key=lambda x: x['confidence'] * 0.7 + x['thai_chars'] * 0.3)
        return None

    # ============================================================
    # 🔹 Post-processing แก้คำผิดบ่อย
    # ============================================================
    def post_process_thai_document(self, text):
        result = text
        common_errors = {
            'คทะ': 'คณะ', 'ทท': 'นัก', 'กท': 'กำ', 'สำทท': 'สำนัก',
            'เรทอง': 'เรื่อง', 'วทที่': 'วันที่', 'เลขที': 'เลขที่',
            'ถท': 'ถึง', 'อทงถท': 'อ้างถึง', 'ทาการ': 'ทำการ',
            'กาหนด': 'กำหนด', 'ดาเนิน': 'ดำเนิน', 'สาคัญ': 'สำคัญ',
            'สาหรับ': 'สำหรับ', 'คานำ': 'คำนำ', 'คาสั่ง': 'คำสั่ง',
            'คาแนะนำ': 'คำแนะนำ', 'คาขอ': 'คำขอ', 'คารับรอง': 'คำรับรอง',
            'คาระบุ': 'คำระบุ', 'นาเสนอ': 'นำเสนอ', 'ดารง': 'ดำรง',
            'คาร้อง': 'คำร้อง', 'คาตอบ': 'คำตอบ', 'คาพิพากษา': 'คำพิพากษา',
            'สาเนา': 'สำเนา', 'สาเร็จ': 'สำเร็จ', 'คาแถลง': 'คำแถลง',
            'คาอธิบาย': 'คำอธิบาย', 'คาเตือน': 'คำเตือน', 'ทางาน': 'ทำงาน',
            'คาเสนอ': 'คำเสนอ', 'คาแปล': 'คำแปล',
        }
        for wrong, correct in common_errors.items():
            result = result.replace(wrong, correct)

        # ล้างช่องว่างเกิน
        result = re.sub(r'([ก-ฮ])\s+([ะ-ู])', r'\1\2', result)
        result = re.sub(r'([ั-ู])\s+([ก-ฮ])', r'\1\2', result)
        result = re.sub(r' +', ' ', result)
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        return result.strip()

    # ============================================================
    # 🔹 แก้คำผิดภาษาไทยอัตโนมัติ (ใช้ PyThaiNLP)
    # ============================================================
    def correct_thai_spelling(self, text):
        try:
            from pythainlp import spell
            from pythainlp.tokenize import word_tokenize
        except ImportError:
            print("⚠️ ต้องติดตั้ง PyThaiNLP ก่อน: pip install pythainlp")
            return text

        words = word_tokenize(text, engine="newmm")
        corrected = []
        for w in words:
            if not re.match(r'^[ก-๙]+$', w):
                corrected.append(w)
                continue
            suggestion = spell(w)
            if suggestion and suggestion[0] != w:
                corrected.append(suggestion[0])
            else:
                corrected.append(w)
        return ''.join(corrected).strip()





    def extract_key_fields(self, text):
        """
        ดึงข้อมูลสำคัญจากเอกสารราชการภาษาไทย
        """
        fields = {}
    
        # ============================================================
        # 🔹 เลขที่หนังสือ — รองรับหลายรูปแบบ (มีหรือไม่มีคำว่า "เลขที่")
        # ============================================================
        match = re.search(
            r'(?:บ\s*)?(?:เ[อน][็น]ที|เอ็นที|ศธ|นท|งป|คส|นพ|ผส)\S*\/\S*(?:\s*วันที่\s*\d{1,2}\s*[ก-๙]+\s*\d{4})?',
            text
        )
        if match:
            number_text = match.group(0).strip()
            number_text = re.sub(r'^บ\s*', '', number_text)  # ตัดคำว่า "บ" ด้านหน้าออก
    
            # เพิ่ม "เอ็นที" ถ้ายังไม่มี
            if not number_text.startswith("เอ็นที"):
                number_text = re.sub(r'^(เ[อน][็น]ที)', 'เอ็นที', number_text)  # normalize ตัวสะกด
                if not number_text.startswith("เอ็นที"):
                    number_text = f"เอ็นที{number_text}"
    
            fields["เลขที่หนังสือ"] = number_text
    
        else:
            # fallback: ถ้ามีคำว่า "เลขที่"
            match = re.search(r'เลขที่[:\s]*([^\n]+)', text)
            if match:
                num = match.group(1).strip()
                if not num.startswith("เอ็นที"):
                    num = f"เอ็นที{num}"
                fields['เลขที่หนังสือ'] = num
    
        # ============================================================
        # 🔹 วันที่หนังสือ
        # ============================================================
        match = re.search(r'วันที่[:\s]*([^\n]+)', text)
        if match:
            fields['วันที่หนังสือ'] = match.group(1).strip()
    
        # ============================================================
        # 🔹 เรื่อง
        # ============================================================
        match = re.search(r'เรื่อง[:\s]*([^\n]+(?:\n(?!\s*เรียน)[^\n]+)*)', text)
        if match:
            fields['เรื่อง'] = match.group(1).strip()
    
        # ============================================================
        # 🔹 เรียน / ผู้รับ
        # ============================================================
        match = re.search(r'เรียน[:\s]*([^\n]+)', text)
        if match:
            fields['เรียน'] = match.group(1).strip()
    
        # ============================================================
        # 🔹 เนื้อหา (3–5 บรรทัดหลังคำว่า “เรียน”)
        # ============================================================
        body_match = re.search(r'เรียน[:\s]*[^\n]+\n(.*)', text, re.DOTALL)
        if body_match:
            body_lines = body_match.group(1).strip().splitlines()
            preview = "\n".join(body_lines[:5])  # แสดงเฉพาะ 5 บรรทัดแรก
            preview = re.sub(r'\s{2,}', ' ', preview)  # ล้างช่องว่างซ้ำ
            fields['เนื้อหา'] = preview.strip()
    
        return fields
 

 

    # ============================================================
    # 🔹 Pipeline หลัก
    # ============================================================
    def process_document(self, file_path, save_debug=True, from_pdf=False):
        print(f"\n{'='*60}\nProcessing: {file_path}\n{'='*60}\n")
        is_pdf = file_path.lower().endswith('.pdf')

        # 🔸 PDF mode
        if is_pdf and PDF_SUPPORT:
            if self.check_pdf_has_text(file_path):
                text = self.extract_text_from_pdf(file_path)
                if text:
                    cleaned = self.correct_thai_spelling(self.post_process_thai_document(text))
                    return {
                        'text': cleaned,
                        'key_fields': self.extract_key_fields(cleaned),
                        'method': 'PDF text layer',
                        'confidence': 100.0
                    }

            # 🔸 ไม่มี text layer
            print("✓ No text layer found - converting to images for OCR...")
            imgs = self.pdf_to_images(file_path, dpi=300)
            results = [self._process_image(img, save_debug, True) for img in imgs if img]
            valid = [r for r in results if r]
            if not valid: return None
            combined = '\n\n--- หน้าใหม่ ---\n\n'.join(r['text'] for r in valid)
            avg_conf = sum(r['confidence'] for r in valid) / len(valid)
            cleaned = self.post_process_thai_document(combined)
            return {
                'text': cleaned,
                'key_fields': self.extract_key_fields(cleaned),
                'method': 'PDF OCR',
                'confidence': avg_conf,
                'pages': len(valid)
            }

        # 🔹 Image mode
        return self._process_image(file_path, save_debug, from_pdf)

    # ============================================================
    # 🔹 Process ภาพเดี่ยว
    # ============================================================
    def _process_image(self, image_path, save_debug, high_quality=False):
        processed = self.preprocess_for_high_quality(image_path) if high_quality else self.preprocess_for_low_quality(image_path)
        if save_debug:
            cv2.imwrite(image_path.replace('.', '_debug.'), processed)
        result = self.ocr_with_multiple_configs(processed)
        if not result: return None
        cleaned = self.correct_thai_spelling(self.post_process_thai_document(result['text']))
        return {
            'text': cleaned,
            'key_fields': self.extract_key_fields(cleaned),
            'confidence': result['confidence']
        }


# ============================================================
# 🔸 ทดสอบ standalone
# ============================================================
if __name__ == "__main__":
    ocr = EnhancedThaiDocumentOCR()
    test_file = "document.pdf"
    try:
        result = ocr.process_document(test_file, save_debug=True)
        if result:
            print("\n=== OCR SUCCESS ===")
            print(f"Method: {result.get('method')}")
            print(f"Confidence: {result['confidence']:.2f}%")
            print("Key Fields:", result['key_fields'])
            print("\nText:\n", result['text'][:800], "...")
    except Exception as e:
        print(f"Error: {e}")





