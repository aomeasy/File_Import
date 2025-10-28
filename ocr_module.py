#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Thai OCR with PDF Support
รองรับทั้ง PDF และไฟล์ภาพ
"""

import cv2
import numpy as np
import pytesseract
from PIL import Image, ImageEnhance
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
    
    def check_pdf_has_text(self, pdf_path):
        """
        ตรวจสอบว่า PDF มี text layer หรือไม่
        """
        if not PDF_SUPPORT:
            return False
        
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                # ตรวจสอบ 3 หน้าแรก
                pages_to_check = min(3, len(pdf_reader.pages))
                
                for i in range(pages_to_check):
                    page = pdf_reader.pages[i]
                    text = page.extract_text()
                    
                    # ถ้ามีอักษรไทยมากกว่า 50 ตัว ถือว่ามี text layer
                    thai_chars = len(re.findall(r'[ก-ฮะ-์]', text))
                    if thai_chars > 50:
                        return True
                
                return False
                
        except Exception as e:
            print(f"Error checking PDF: {e}")
            return False
    
    def extract_text_from_pdf(self, pdf_path):
        """
        Extract text จาก PDF ที่มี text layer
        """
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
                
                combined_text = '\n\n'.join(all_text)
                return combined_text
                
        except Exception as e:
            print(f"✗ Error extracting text: {e}")
            return None
    
    def pdf_to_images(self, pdf_path, output_folder='temp_pages', dpi=300):
        """
        แปลง PDF เป็นภาพ (สำหรับ PDF ที่ไม่มี text layer)
        """
        if not PDF_SUPPORT:
            raise ImportError("PyMuPDF not installed")
        
        print(f"Converting PDF to images (DPI={dpi})...")
        
        # สร้าง folder
        os.makedirs(output_folder, exist_ok=True)
        
        try:
            doc = fitz.open(pdf_path)
            image_paths = []
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                
                # Render page เป็นภาพ (DPI สูง!)
                zoom = dpi / 72  # 72 is default DPI
                mat = fitz.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                
                # บันทึกเป็น PNG
                output_path = os.path.join(output_folder, f'page_{page_num+1}.png')
                pix.save(output_path)
                image_paths.append(output_path)
                
                print(f"  ✓ Page {page_num+1}/{len(doc)} saved: {output_path}")
            
            doc.close()
            return image_paths
            
        except Exception as e:
            print(f"✗ Error converting PDF: {e}")
            return []
    
    def preprocess_for_low_quality(self, image_path):
        """
        Preprocessing สำหรับภาพคุณภาพต่ำ
        """
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Cannot read image: {image_path}")
        
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Resize
        height, width = gray.shape
        target_height = 3500
        if height < target_height:
            scale = target_height / height
            new_width = int(width * scale)
            new_height = int(height * scale)
            gray = cv2.resize(gray, (new_width, new_height), 
                            interpolation=cv2.INTER_LANCZOS4)
        
        # Bilateral filter + Denoise
        bilateral = cv2.bilateralFilter(gray, 9, 75, 75)
        denoised = cv2.fastNlMeansDenoising(bilateral, None, h=20, 
                                            templateWindowSize=7, 
                                            searchWindowSize=21)
        
        # Sharpen
        kernel_sharpen = np.array([[-1, -1, -1], [-1, 9, -1], [-1, -1, -1]])
        sharpened = cv2.filter2D(denoised, -1, kernel_sharpen)
        
        # CLAHE
        clahe = cv2.createCLAHE(clipLimit=4.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(sharpened)
        
        # Unsharp masking
        gaussian = cv2.GaussianBlur(enhanced, (0, 0), 2.0)
        unsharp = cv2.addWeighted(enhanced, 1.5, gaussian, -0.5, 0)
        
        # Adaptive thresholding
        binary = cv2.adaptiveThreshold(unsharp, 255, 
                                       cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY, 
                                       blockSize=21, C=2)
        
        # Morphological operations
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2, 2))
        binary = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
        binary = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel)
        
        return binary
    
    def preprocess_for_high_quality(self, image_path):
        """
        Preprocessing สำหรับภาพจาก PDF (คุณภาพสูง)
        ไม่ต้อง aggressive มาก
        """
        img = cv2.imread(image_path)
        if img is None:
            raise ValueError(f"Cannot read image: {image_path}")
        
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Resize (ถ้าจำเป็น)
        height, width = gray.shape
        if height < 2500:
            scale = 2500 / height
            new_width = int(width * scale)
            new_height = int(height * scale)
            gray = cv2.resize(gray, (new_width, new_height), 
                            interpolation=cv2.INTER_CUBIC)
        
        # Denoise เบาๆ
        denoised = cv2.fastNlMeansDenoising(gray, None, h=10, 
                                            templateWindowSize=7, 
                                            searchWindowSize=21)
        
        # CLAHE
        clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
        enhanced = clahe.apply(denoised)
        
        # Adaptive thresholding
        binary = cv2.adaptiveThreshold(enhanced, 255, 
                                       cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY, 
                                       blockSize=15, C=4)
        
        # Slight morphology
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 1))
        binary = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
        
        return binary
    
    def ocr_with_multiple_configs(self, image):
        """
        ลอง OCR หลาย config
        """
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
                data = pytesseract.image_to_data(image, config=config,
                                                output_type=pytesseract.Output.DICT)
                
                confs = [int(c) for c in data['conf'] if str(c) != '-1']
                avg_conf = sum(confs) / len(confs) if confs else 0
                thai_chars = len(re.findall(r'[ก-ฮะ-์]', text))
                
                results.append({
                    'text': text,
                    'confidence': avg_conf,
                    'thai_chars': thai_chars,
                    'config': config
                })
                
            except Exception as e:
                pass
        
        if results:
            best = max(results, key=lambda x: x['confidence'] * 0.7 + x['thai_chars'] * 0.3)
            return best
        
        return None
    
    def post_process_thai_document(self, text):
        """
        Post-processing
        """
        result = text
        
        # แก้ไขคำที่อ่านผิดบ่อย
      

        common_errors = {
            # 🔹 คำทั่วไป
            'คทะ': 'คณะ', 'ทท': 'นัก', 'กท': 'กำ', 'สำทท': 'สำนัก',
            'เรทอง': 'เรื่อง', 'วทที่': 'วันที่', 'เลขที': 'เลขที่',
            'ถท': 'ถึง', 'อทงถท': 'อ้างถึง', 'อ้ งถท': 'อ้างถึง',
            'ค ะ': 'คณะ', 'ท ง น': 'ทำงาน', 'ส ง': 'ส่ง',
            'เรท ย': 'เรียน', 'มห วท': 'มหาวิทยาลัย',

            # 🔹 คำที่มีสระ "ำ" (OCR มักอ่านผิด)
            'ดาเนิน': 'ดำเนิน',
            'ดาเนินการ': 'ดำเนินการ',
            'ดาเนินงาน': 'ดำเนินงาน',
            'กาหนด': 'กำหนด',
            'กากับ': 'กำกับ',
            'กาชับ': 'กำชับ',
            'จานวน': 'จำนวน',
            'กาจัด': 'กำจัด',
            'นาเสนอ': 'นำเสนอ',
            'กาลัง': 'กำลัง',
            'ทางาน': 'ทำงาน',
            'กามะลอ': 'กำมะลอ',
            'กาสรด': 'กำสรด',
            'ทางาน': 'ทำงาน',
            'ทาการ': 'ทำการ',
            'ทารายงาน': 'ทำรายงาน',
            'ทาหนังสือ': 'ทำหนังสือ',
            'ทาสัญญา': 'ทำสัญญา',
            'คานำ': 'คำนำ',
            'คาสั่ง': 'คำสั่ง',
            'คาชี้แจง': 'คำชี้แจง',
            'คาอธิบาย': 'คำอธิบาย',
            'คาตอบ': 'คำตอบ',
            'คาแนะนำ': 'คำแนะนำ',
            'คาร้อง': 'คำร้อง',
            'คาขอ': 'คำขอ',
            'คากล่าว': 'คำกล่าว',
            'คาพิพากษา': 'คำพิพากษา',
            'สาคัญ': 'สำคัญ',
            'สาเร็จ': 'สำเร็จ',
            'สาหรับ': 'สำหรับ',
            'สานัก': 'สำนัก',
            'สานึก': 'สำนึก',
            'สาแดง': 'สำแดง',
            'สาเนา': 'สำเนา',
            'สาเร็จรูป': 'สำเร็จรูป',
            'สาเร็จการศึกษา': 'สำเร็จการศึกษา',
            'สาเร็จราชการ': 'สำเร็จราชการ',
            'สาหรับการ': 'สำหรับการ',
            'ดารง': 'ดำรง',
            'ดารงตำแหน่ง': 'ดำรงตำแหน่ง',
            'ดารงชีวิต': 'ดำรงชีวิต',
            'ดารงชีพ': 'ดำรงชีพ',
            'ดารงอยู่': 'ดำรงอยู่',
            'กานด': 'กำหนด',  # OCR variant
            'กาหนดการ': 'กำหนดการ',
            'กาหนดเวลา': 'กำหนดเวลา',
            'กาหนดวัน': 'กำหนดวัน',
            'กาหนดส่ง': 'กำหนดส่ง',
            'กาหนดเสร็จสิ้น': 'กำหนดเสร็จสิ้น',
            'สาหรับการใช้งาน': 'สำหรับการใช้งาน',
            'สาหรับลูกค้า': 'สำหรับลูกค้า',
            'สาหรับผู้ใช้': 'สำหรับผู้ใช้',
            'สาหรับเจ้าหน้าที่': 'สำหรับเจ้าหน้าที่',
            'สาหรับส่วนงาน': 'สำหรับส่วนงาน',
            'สาหรับโครงการ': 'สำหรับโครงการ',
            'สาหรับงาน': 'สำหรับงาน',
            'กาหนดค่า': 'กำหนดค่า',
            'กาหนดให้': 'กำหนดให้',
            'สาเนารายงาน': 'สำเนารายงาน',
            'สาเนาหนังสือ': 'สำเนาหนังสือ',
            'สาเนาเอกสาร': 'สำเนาเอกสาร',
            'สาเนาข้อมูล': 'สำเนาข้อมูล',
            'สาเนาผลการดำเนินงาน': 'สำเนาผลการดำเนินงาน',
            'สานวน': 'สำนวน',
            'สานวนการ': 'สำนวนการ',
            'สานวนนโยบาย': 'สำนวน',
            'คาแถลง': 'คำแถลง',
            'คารับรอง': 'คำรับรอง',
            'คาเตือน': 'คำเตือน',
            'คาระบุ': 'คำระบุ',
            'คารับ': 'คำรับ',
            'คาเสนอ': 'คำเสนอ',
            'คาแปล': 'คำแปล',

            # ===== คำราชการเฉพาะ =====
            'เลขที่: บ่': 'เลขที่: ',
            'ชาญ.ในป': 'ชจญ.นป.',
            'พรสวรรค์': 'นครสวรรค์',  # OCR เพี้ยน
            'รายงาน ป': 'รายงาน นป.',
            'คณะทางาน': 'คณะทำงาน',
            'ทางานฯฯ': 'ทำงานฯ',
            'ทางานฯ': 'ทำงานฯ',
            'คณะทางานฯฯ': 'คณะทำงานฯ',
            'คณะทางานฯฯฯ': 'คณะทำงานฯ',
            'คณะฯฯ': 'คณะฯ',
            'ฯฯ': 'ฯ',
            'ลว': 'ลงวันที่',
            'นาเสนอ': 'นำเสนอ',
            'นาใช้': 'นำใช้',
            'นา AI': 'นำ AI',
            'นาความรู้': 'นำความรู้',
            'นาประยุกต์': 'นำประยุกต์',
            'นาเสนอผลงาน': 'นำเสนอผลงาน',
            'นาองค์ความรู้': 'นำองค์ความรู้',
            'นาเทคโนโลยี': 'นำเทคโนโลยี',

            # ===== คำที่สลับสระ/ตกหล่น =====
            'โปรดปราน': 'โปรดทราบ',
            'ในที่ประชุมรายงาน ป': 'ในที่ประชุมรายงาน นป.',
            'กาหนดี': 'กำหนด',
            'กาหนดทิศทาง': 'กำหนดทิศทาง',
            'กาหนดการ': 'กำหนดการ',
            'กาหนดเวลา': 'กำหนดเวลา',
            'กาหนดวัน': 'กำหนดวัน',
            'สาเหตุ': 'สาเหตุ',  # ป้องกันแก้เกิน
            'คาสั่งที่': 'คำสั่งที่',
            'บทนา': 'บทนำ',
            'แนะนา': 'แนะนำ',
            'นาเสนอแนวทาง': 'นำเสนอแนวทาง',
            'แนะนาการ': 'แนะนำการ',
            'ทากิจกรรม': 'ทำกิจกรรม',
            'ทาการประชุม': 'ทำการประชุม',
            'ทาการอบรม': 'ทำการอบรม',
            'ทาการจัดทำ': 'ทำการจัดทำ',

            # ===== ช่วยเพิ่มความถูกต้องเชิงโครงสร้าง =====
            'วัน เดือน ปีรายการกิจกรรม': 'วัน เดือน ปี รายการกิจกรรม',
            'การดาเนินงาน': 'การดำเนินงาน',
            'ผลการดาเนินงาน': 'ผลการดำเนินงาน',
            'การดาเนินการ': 'การดำเนินการ',
            'โดยสรุป การดาเนินงาน': 'โดยสรุป การดำเนินงาน',
            'ผลการประชุมคณะทางาน': 'ผลการประชุมคณะทำงาน',
            'คณะทางานด้าน': 'คณะทำงานด้าน',
            'รายงานผลการดาเนินงาน': 'รายงานผลการดำเนินงาน',
            'คณะทางานจัดการด้าน': 'คณะทำงานจัดการด้าน',
            'คณะทางานด้านปัญญาประดิษฐ์': 'คณะทำงานด้านปัญญาประดิษฐ์',
            'ระบบรายงานอัจฉริยะสาหรับลูกค้า': 'ระบบรายงานอัจฉริยะสำหรับลูกค้า',
            'ระบบผู้ช่วยอัจฉริยะด้านการขาย': 'ระบบผู้ช่วยอัจฉริยะด้านการขาย',
        }
        
        for wrong, correct in common_errors.items():
            result = result.replace(wrong, correct)
        
        # แก้ spacing
        result = re.sub(r'([ก-ฮ])\s+([ะ-ู])', r'\1\2', result)
        result = re.sub(r'([ั-ู])\s+([ก-ฮ])', r'\1\2', result)
        result = re.sub(r'\s*:\s*', ': ', result)
        result = re.sub(r' +', ' ', result)
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        
        return result.strip()


    def correct_thai_spelling(self, text):
        """
        ตรวจจับและแก้ไขคำผิดภาษาไทย (ใช้ PyThaiNLP)
        """
        try:
            from pythainlp import spell
            from pythainlp.tokenize import word_tokenize
        except ImportError:
            print("⚠️ ต้องติดตั้ง PyThaiNLP ก่อน: pip install pythainlp")
            return text

        # แบ่งคำออกก่อน
        words = word_tokenize(text, engine="newmm")

        corrected_words = []
        for w in words:
            # ข้ามคำที่ไม่ใช่ตัวอักษรไทย เช่น ตัวเลข, สัญลักษณ์
            if not re.match(r'^[ก-๙]+$', w):
                corrected_words.append(w)
                continue

            # ตรวจว่าคำนี้อยู่ในพจนานุกรมไหม
            suggested = spell(w)
            if suggested and suggested[0] != w:
                corrected_words.append(suggested[0])
            else:
                corrected_words.append(w)

        corrected_text = ''.join(corrected_words)

        # แก้ spacing ซ้ำอีกครั้ง (กันติดกันเกิน)
        corrected_text = re.sub(r'([ก-ฮะ-์])([A-Za-z0-9])', r'\1 \2', corrected_text)
        corrected_text = re.sub(r' +', ' ', corrected_text)

        return corrected_text.strip()

    
    def extract_key_fields(self, text):
        """
        ดึงข้อมูลสำคัญ
        """
        fields = {}
        
        # เลขที่
        match = re.search(r'เลขที่[:\s]*([^\n]+)', text)
        if match:
            fields['เลขที่'] = match.group(1).strip()
        
        # วันที่
        match = re.search(r'วันที่[:\s]*(\d+[^\n]+)', text)
        if match:
            fields['วันที่'] = match.group(1).strip()
        
        # เรื่อง
        match = re.search(r'เรื่อง[:\s]*([^\n]+(?:\n(?!\s*เรียน)[^\n]+)*)', text)
        if match:
            fields['เรื่อง'] = match.group(1).strip()
        
        # เรียน
        match = re.search(r'เรียน[:\s]*([^\n]+)', text)
        if match:
            fields['เรียน'] = match.group(1).strip()
        
        return fields
    
    def process_document(self, file_path, save_debug=True, from_pdf=False):
        """
        Pipeline หลัก - รองรับทั้ง PDF และภาพ
        """
        print(f"\n{'='*60}")
        print(f"Processing: {file_path}")
        print(f"{'='*60}\n")
        
        # ตรวจสอบว่าเป็น PDF หรือไม่
        is_pdf = file_path.lower().endswith('.pdf')
        
        if is_pdf and PDF_SUPPORT:
            # 1. ลองดึง text จาก PDF ก่อน
            has_text = self.check_pdf_has_text(file_path)
            
            if has_text:
                text = self.extract_text_from_pdf(file_path)
                if text:
                    cleaned = self.post_process_thai_document(text)
                    cleaned = self.correct_thai_spelling(cleaned)
                    fields = self.extract_key_fields(cleaned)
                    
                    print(f"\n{'='*60}")
                    print("RESULTS (from PDF text layer)")
                    print(f"{'='*60}\n")
                    
                    print("--- Key Fields ---")
                    for key, value in fields.items():
                        print(f"{key}: {value}")
                    
                    print("\n--- Full Text ---")
                    print(cleaned)
                    
                    return {
                        'text': cleaned,
                        'key_fields': fields,
                        'method': 'PDF text extraction',
                        'confidence': 100.0
                    }
            
            # 2. ถ้าไม่มี text layer -> แปลงเป็นภาพ
            print("✓ No text layer found - converting to images for OCR...")
            image_paths = self.pdf_to_images(file_path, dpi=300)
            
            if not image_paths:
                print("✗ Failed to convert PDF to images")
                return None
            
            # OCR ทุกหน้า
            all_results = []
            for img_path in image_paths:
                result = self._process_image(img_path, save_debug, high_quality=True)
                if result:
                    all_results.append(result)
            
            # รวมผลลัพธ์
            if all_results:
                combined_text = '\n\n--- หน้าใหม่ ---\n\n'.join([r['text'] for r in all_results])
                cleaned = self.post_process_thai_document(combined_text)
                fields = self.extract_key_fields(cleaned)
                
                avg_conf = sum([r['confidence'] for r in all_results]) / len(all_results)
                
                print(f"\n{'='*60}")
                print("COMBINED RESULTS")
                print(f"{'='*60}")
                print(f"Pages processed: {len(all_results)}")
                print(f"Average confidence: {avg_conf:.2f}%")
                
                print("\n--- Key Fields ---")
                for key, value in fields.items():
                    print(f"{key}: {value}")
                
                print("\n--- Full Text ---")
                print(cleaned)
                
                return {
                    'text': cleaned,
                    'key_fields': fields,
                    'method': 'PDF OCR',
                    'confidence': avg_conf,
                    'pages': len(all_results)
                }
        
        else:
            # ไฟล์ภาพ
            return self._process_image(file_path, save_debug, high_quality=from_pdf)
    
    def _process_image(self, image_path, save_debug, high_quality=False):
        """
        ประมวลผลภาพเดี่ยว
        """
        # เลือก preprocessing ตามคุณภาพ
        if high_quality:
            processed = self.preprocess_for_high_quality(image_path)
        else:
            processed = self.preprocess_for_low_quality(image_path)
        
        if save_debug:
            debug_path = image_path.replace('.', '_debug.')
            cv2.imwrite(debug_path, processed)
        
        # OCR
        result = self.ocr_with_multiple_configs(processed)
        
        if not result:
            return None
        
        cleaned = self.post_process_thai_document(result['text'])
        cleaned = self.correct_thai_spelling(cleaned)
        fields = self.extract_key_fields(cleaned)
        
        return {
            'text': cleaned,
            'key_fields': fields,
            'confidence': result['confidence']
        }


# ===== ตัวอย่างการใช้งาน =====

def main():
    ocr = EnhancedThaiDocumentOCR()
    
    # เปลี่ยนเป็น path ของคุณ
    file_path = "document.pdf"  # หรือ "document.jpg"
    
    try:
        result = ocr.process_document(file_path, save_debug=True)
        
        if result:
            print(f"\n\n{'='*60}")
            print("SUCCESS!")
            print(f"{'='*60}")
            print(f"Method: {result.get('method', 'Image OCR')}")
            print(f"Confidence: {result['confidence']:.2f}%")
            
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()