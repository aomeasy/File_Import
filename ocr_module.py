#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Thai OCR with Gemini API
ใช้ Google Gemini Vision API สำหรับ OCR ภาษาไทย
"""

import os
import re
import base64
from pathlib import Path
import google.generativeai as genai

# สำหรับ PDF
try:
    import PyPDF2
    import fitz  # PyMuPDF
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    print("⚠️  PDF libraries not installed. Install with:")
    print("   pip install PyPDF2 PyMuPDF")


class GeminiThaiDocumentOCR:
    def __init__(self, api_key=None):
        """
        Initialize Gemini OCR
        
        Parameters:
        -----------
        api_key : str, optional
            Google API key. ถ้าไม่ระบุจะอ่านจาก environment variable GOOGLE_API_KEY
        """
        self.api_key = api_key or os.getenv('GOOGLE_API_KEY')
        if not self.api_key:
            raise ValueError(
                "ต้องระบุ API key ผ่าน parameter หรือตั้งค่า environment variable GOOGLE_API_KEY\n"
                "รับ API key ได้ที่: https://makersuite.google.com/app/apikey"
            )
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
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
    # 🔹 OCR ด้วย Gemini Vision API
    # ============================================================




    def ocr_with_gemini(self, image_path):
        """ใช้ Gemini Vision API อ่านข้อความจากภาพ"""
        try:
            print(f"📸 Processing with Gemini API: {os.path.basename(image_path)}")
            
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            # ✅ ปรับ prompt ให้ระบุชัดเจนเรื่องสระ "ำ"
            prompt = """คุณเป็น OCR expert สำหรับเอกสารราชการภาษาไทย

กรุณาอ่านข้อความทั้งหมดในภาพนี้อย่างละเอียดและถูกต้องที่สุด:

**คำแนะนำสำคัญ:**
1. อ่านข้อความภาษาไทยทุกตัวอักษรให้ถูกต้อง รวมทั้งสระ วรรณยุกต์
2. **⚠️ ระวังสระ "ำ" (sara am) เป็นพิเศษ** - ห้ามสับสนกับ "า" + "ม"
   ตัวอย่างคำที่มี "ำ": กำหนด, สำคัญ, สำหรับ, คำ, นำ, ทำ
3. รักษาโครงสร้างเอกสารเดิม (บรรทัดใหม่, ช่องว่าง, การจัดวาง)
4. ระวังคำที่มักอ่านผิด เช่น:
   - กำหนด (ไม่ใช่ กาหนด)
   - สำคัญ (ไม่ใช่ สาคัญ)
   - ทำการ (ไม่ใช่ ทาการ)
   - คำสั่ง (ไม่ใช่ คาสั่ง)
5. ระวังตัวย่อหน่วยงาน เช่น "ชจญ.นป." "ผส.สสบป." "บนป."
6. เลขที่หนังสือมักขึ้นต้นด้วย "เอ็นที" หรือ "ศธ" หรือ "นท"
7. วันที่มักเป็นเดือนภาษาไทย เช่น "15 มกราคม 2568"

**ส่งคืนเฉพาะข้อความที่อ่านได้ โดยไม่ต้องมีคำอธิบายเพิ่มเติม**"""
        
            # เรียกใช้ Gemini API
            response = self.model.generate_content([
                prompt,
                {
                    'mime_type': 'image/png' if image_path.lower().endswith('.png') else 'image/jpeg',
                    'data': image_data
                }
            ])
            
            text = response.text.strip()
            
            # ✅ เพิ่ม post-processing ทันที
            text = self.post_process_thai_document(text)
            
            thai_chars = len(re.findall(r'[ก-ฮะ-์]', text))
            confidence = min(95.0, 70.0 + (thai_chars / 10))
            
            print(f"  ✓ Extracted {len(text)} characters ({thai_chars} Thai chars)")
            
            return {
                'text': text,
                'confidence': confidence
            }
            
        except Exception as e:
            print(f"  ✗ Gemini API Error: {e}")
            return None
        
     

    # ============================================================
    # 🔹 Post-processing แก้คำผิดบ่อย
    # ============================================================




    def post_process_thai_document(self, text):
        """
        Post-processing แก้คำผิดบ่อย รวมถึงสระ ำ ที่อ่านผิด
        """
        result = text
        
        # ==========================================
        # 🔥 แก้ไขสระ "ำ" ที่ OCR อ่านผิดเป็น "า" + "ม"
        # ==========================================
        sara_am_fixes = {
            # คำที่มี "ำ" แต่ OCR อ่านเป็น "าม"
            'กาหนด': 'กำหนด',
            'กาลัง': 'กำลัง',
            'ดาเนิน': 'ดำเนิน',
            'นาเสนอ': 'นำเสนอ',
            'สาคัญ': 'สำคัญ',
            'สาหรับ': 'สำหรับ',
            'สาเนา': 'สำเนา',
            'สาเร็จ': 'สำเร็จ',
            'สาทท': 'สำนัก',
            'คานำ': 'คำนำ',
            'คาสั่ง': 'คำสั่ง',
            'คาแนะนำ': 'คำแนะนำ',
            'คาขอ': 'คำขอ',
            'คารับรอง': 'คำรับรอง',
            'คาระบุ': 'คำระบุ',
            'คาร้อง': 'คำร้อง',
            'คาตอบ': 'คำตอบ',
            'คาพิพากษา': 'คำพิพากษา',
            'คาแถลง': 'คำแถลง',
            'คาอธิบาย': 'คำอธิบาย',
            'คาเตือน': 'คำเตือน',
            'คาเสนอ': 'คำเสนอ',
            'คาแปล': 'คำแปล',
            'ทาการ': 'ทำการ',
            'ทางาน': 'ทำงาน',
            'ดารง': 'ดำรง',
            'ราคา': 'ราคา',  # นี่ถูกต้องแล้ว (ไม่แก้)
            'รามา': 'รามา',  # ชื่อเฉพาะ (ไม่แก้)
            
            # คำเฉพาะจากเอกสารราชการ
            'อางถึง': 'อ้างถึง',
            'เรทอง': 'เรื่อง',
            'วทที่': 'วันที่',
            'ถท': 'ถึง',
            
            # หน่วยงาน NT
            'กท': 'กำ',
            'คทะ': 'คณะ',
        }
        
        # แก้ไขคำผิด
        for wrong, correct in sara_am_fixes.items():
            result = result.replace(wrong, correct)
        
        # ==========================================
        # 🔍 Pattern-based fixing (ใช้ regex)
        # ==========================================
        import re
        
        # แก้ไข "าม" → "ำ" สำหรับคำที่มักผิด
        # เช่น "กาหนด" → "กำหนด"
        common_patterns = [
            (r'กา(หนด|ลัง)', r'กำ\1'),           # กาหนด → กำหนด, กาลัง → กำลัง
            (r'ดา(เนิน|รง)', r'ดำ\1'),           # ดาเนิน → ดำเนิน
            (r'นา(เสนอ)', r'นำ\1'),              # นาเสนอ → นำเสนอ
            (r'สา(คัญ|หรับ|เนา|เร็จ|ทท)', r'สำ\1'),  # สาคัญ → สำคัญ
            (r'คา([นสขรตพแอเ]\S*)', r'คำ\1'),   # คา... → คำ...
            (r'ทา(การ|งาน)', r'ทำ\1'),           # ทาการ → ทำการ
        ]
        
        for pattern, replacement in common_patterns:
            result = re.sub(pattern, replacement, result)
        
        # ==========================================
        # ✅ ส่วนเดิม (คงไว้)
        # ==========================================
        
        # ล้างช่องว่างเกิน
        result = re.sub(r'([ก-ฮ])\s+([ะ-ู])', r'\1\2', result)
        result = re.sub(r'([ั-ู])\s+([ก-ฮ])', r'\1\2', result)
        result = re.sub(r' +', ' ', result)
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        
        # แก้คำย่อหน่วยงาน
        unit_corrections = {
            "ชาญ.ในป.": "ชจญ.นป.",
            "ชาน.นป.": "ชจญ.นป.",
            "ชาญ.นป.": "ชจญ.นป.",
            "ผส.สสพบป.": "ผส.สสบป.",
            "ผส.สสพป.": "ผส.สสบป.", 
            "ผส.ลนป.": "ผส.นป.",
            "ผส.บลน.": "ผส.บนป.",
            "บลนป.": "บนป.", 
            "ชาน.ในป.": "ชจญ.นป.",
            "ผส.สบในป.": "ผส.สบนป.", 
        }
        
        for wrong, correct in unit_corrections.items():
            result = result.replace(wrong, correct)
        
        return result.strip()
     

    # ============================================================
    # 🔹 ดึงข้อมูลสำคัญจากเอกสาร
    # ============================================================
    def extract_key_fields(self, text):
        """ดึงข้อมูลสำคัญจากเอกสารราชการภาษาไทย"""
        fields = {}
    
        # เลขที่หนังสือ
        match = re.search(
            r'(?:บ\s*)?(?:เ[อน][็น]ที|เอ็นที|ศธ|นท|งป|คส|นพ|ผส)\S*\/\S*(?:\s*วันที่\s*\d{1,2}\s*[ก-๙]+\s*\d{4})?',
            text
        )
        if match:
            number_text = match.group(0).strip()
            number_text = re.sub(r'^บ\s*', '', number_text)
            if not number_text.startswith("เอ็นที"):
                number_text = re.sub(r'^(เ[อน][็น]ที)', 'เอ็นที', number_text)
                if not number_text.startswith("เอ็นที"):
                    number_text = f"เอ็นที{number_text}"
            fields["เลขที่หนังสือ"] = number_text
        else:
            match = re.search(r'เลขที่[:\s]*([^\n]+)', text)
            if match:
                num = match.group(1).strip()
                if not num.startswith("เอ็นที"):
                    num = f"เอ็นที{num}"
                fields['เลขที่หนังสือ'] = num
    
        # วันที่หนังสือ
        match = re.search(r'วันที่[:\s]*([^\n]+)', text)
        if match:
            fields['วันที่หนังสือ'] = match.group(1).strip()
    
        # เรื่อง
        match = re.search(r'เรื่อง[:\s]*([^\n]+(?:\n(?!\s*เรียน)[^\n]+)*)', text)
        if match:
            fields['เรื่อง'] = match.group(1).strip()
    
        # เรียน
        match = re.search(r'เรียน[:\s]*([^\n]+)', text)
        if match:
            fields['เรียน'] = match.group(1).strip()
    
        # เนื้อหา (5 บรรทัดแรก)
        body_match = re.search(r'เรียน[:\s]*[^\n]+\n(.*)', text, re.DOTALL)
        if body_match:
            body_lines = body_match.group(1).strip().splitlines()
            preview = "\n".join(body_lines[:5])
            preview = re.sub(r'\s{2,}', ' ', preview)
            fields['เนื้อหา'] = preview.strip()
    
        return fields

    # ============================================================
    # 🔹 Pipeline หลัก
    # ============================================================
    def process_document(self, file_path):
        """
        ประมวลผลเอกสาร (รองรับทั้ง PDF และภาพ)
        
        Parameters:
        -----------
        file_path : str
            path ของไฟล์ต้นฉบับ
            
        Returns:
        --------
        dict : ผลลัพธ์ OCR พร้อมข้อมูลสำคัญ
        """
        print(f"\n{'='*60}\nProcessing: {file_path}\n{'='*60}\n")
        is_pdf = file_path.lower().endswith('.pdf')

        # PDF mode
        if is_pdf and PDF_SUPPORT:
            # ลองดึง text layer ก่อน
            if self.check_pdf_has_text(file_path):
                text = self.extract_text_from_pdf(file_path)
                if text:
                    cleaned = self.post_process_thai_document(text)
                    return {
                        'text': cleaned,
                        'key_fields': self.extract_key_fields(cleaned),
                        'method': 'PDF text layer',
                        'confidence': 100.0
                    }

            # ไม่มี text layer -> แปลงเป็นภาพ
            print("✓ No text layer found - converting to images for OCR...")
            imgs = self.pdf_to_images(file_path, dpi=300)
            results = []
            
            for img in imgs:
                if img:
                    result = self.ocr_with_gemini(img)
                    if result:
                        results.append(result)
            
            if not results:
                return None
                
            # รวมผลจากทุกหน้า
            combined = '\n\n--- หน้าใหม่ ---\n\n'.join(r['text'] for r in results)
            avg_conf = sum(r['confidence'] for r in results) / len(results)
            cleaned = self.post_process_thai_document(combined)
            
            return {
                'text': cleaned,
                'key_fields': self.extract_key_fields(cleaned),
                'method': 'PDF OCR (Gemini)',
                'confidence': avg_conf,
                'pages': len(results)
            }

        # Image mode
        result = self.ocr_with_gemini(file_path)
        if not result:
            return None
            
        cleaned = self.post_process_thai_document(result['text'])
        return {
            'text': cleaned,
            'key_fields': self.extract_key_fields(cleaned),
            'method': 'Image OCR (Gemini)',
            'confidence': result['confidence']
        }


# ============================================================
# 🔸 ตัวอย่างการใช้งาน
# ============================================================
if __name__ == "__main__":
    # ตั้งค่า API key (เลือกวิธีใดวิธีหนึ่ง)
    
    # วิธีที่ 1: ส่งผ่าน parameter
    # ocr = GeminiThaiDocumentOCR(api_key="YOUR_API_KEY_HERE")
    
    # วิธีที่ 2: ตั้งค่า environment variable (แนะนำ)
    # export GOOGLE_API_KEY="your_api_key_here"
    ocr = GeminiThaiDocumentOCR()
    
    # ทดสอบกับไฟล์
    test_file = "document.pdf"  # หรือ "document.png"
    
    try:
        result = ocr.process_document(test_file)
        
        if result:
            print("\n" + "="*60)
            print("=== OCR SUCCESS ===")
            print("="*60)
            print(f"Method: {result.get('method')}")
            print(f"Confidence: {result['confidence']:.2f}%")
            
            if 'pages' in result:
                print(f"Pages: {result['pages']}")
            
            print("\n--- Key Fields ---")
            for key, value in result['key_fields'].items():
                print(f"{key}: {value}")
            
            print("\n--- Full Text (Preview) ---")
            preview_text = result['text'][:1000]
            print(preview_text)
            if len(result['text']) > 1000:
                print(f"\n... (total {len(result['text'])} characters)")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()



