# pdf_analyzer.py
import PyPDF2
import os

def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Extrai o texto de um arquivo PDF usando PyPDF2.
    Retorna a string com o texto.
    """
    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {pdf_path}")
    
    text = []
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text.append(page_text)

    return "\n".join(text)
