import glob
import os
import json
import logging
import shutil
from tkinter import Tk, filedialog

import psycopg2  # Para executar o SQL automaticamente

from dask.distributed import Client
from setup_cluster import setup_master_scheduler
from pdf_analyzer import extract_text_from_pdf
from worker import process_document_with_ai
from repo import ContractParser, generate_sql_script
from repo import Contrato

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def execute_sql_script(sql_script: str):
    """
    Executa um script SQL que pode conter múltiplos statements.
    Caso ele possua vários comandos separados por ';', o psycopg2
    normalmente lida bem, mas se tiver problemas, podemos dividi-los manualmente.
    """
    # Ajuste aqui as credenciais do seu banco
    conn = psycopg2.connect(
        host="54.175.112.114",       # Se seu service Docker se chama "postgres"
        port=5433,
        database="compras_ia",
        user="compras",
        password="Sinerji"     # A senha recém-definida
    )
    try:
        with conn.cursor() as cur:
            # Tenta executar todo o script de uma só vez.
            # Se ocorrer erro de parsing, podemos precisar de outro método (split por ';').
            cur.execute(sql_script)
        conn.commit()
    finally:
        conn.close()


def select_pdf_directory():
    """Função para abrir o explorador de arquivos e escolher a pasta de PDFs."""
    root = Tk()
    root.withdraw()  # Esconde a janela principal
    folder_selected = filedialog.askdirectory(title="Selecione a pasta com os arquivos PDF")
    return folder_selected


def move_pdf_to_lidos(pdf_path, dest_dir="PdfLidos"):
    """Move o arquivo PDF para a pasta 'PdfLidos'."""
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)  # Cria a pasta PdfLidos, se não existir

    dest_path = os.path.join(dest_dir, os.path.basename(pdf_path))
    shutil.move(pdf_path, dest_path)
    logger.info(f"Arquivo movido para: {dest_path}")
    return dest_path


def move_pdf_to_error(pdf_path, dest_dir="PdfErros"):
    """Move o arquivo PDF para a pasta 'PdfErros' caso ocorra algum erro ao processá-lo."""
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)  # Cria a pasta PdfErros, se não existir

    dest_path = os.path.join(dest_dir, os.path.basename(pdf_path))
    shutil.move(pdf_path, dest_path)
    logger.info(f"Arquivo movido para a pasta de erros: {dest_path}")
    return dest_path


def process_pdfs_in_batches(pdf_paths, batch_size=3):
    """Função para processar os PDFs em lotes (batch_size)."""
    batches = [pdf_paths[i:i + batch_size] for i in range(0, len(pdf_paths), batch_size)]
    return batches


def main():
    logger.info("Iniciando Master Orchestrator...")

    # 1. Subir o cluster local (master + scheduler)
    cluster, client = setup_master_scheduler()
    logger.info("Cluster (master + scheduler) iniciado com sucesso.")

    # 2. Selecionar o diretório de PDFs com a GUI
    pdf_dir = select_pdf_directory()
    if not pdf_dir:
        logger.warning("Nenhuma pasta foi selecionada. Encerrando.")
        return

    logger.info(f"Foram encontrados PDFs na pasta: {pdf_dir}")
    pdf_paths = glob.glob(os.path.join(pdf_dir, "*.pdf"))
    logger.info(f"Foram encontrados {len(pdf_paths)} arquivo(s) PDF em '{pdf_dir}'.")

    if not pdf_paths:
        logger.warning(f"Nenhum PDF encontrado em '{pdf_dir}'. Encerrando.")
        return

    # 3. Processar PDFs em lotes de 3
    doc_data_list = []
    batches = process_pdfs_in_batches(pdf_paths)

    for batch in batches:
        logger.info(f"Processando lote de {len(batch)} PDFs.")
        
        for i, pdf_path in enumerate(batch, start=1):
            logger.info(f"Lendo [{i}/{len(batch)}]: '{pdf_path}'")
            try:
                text = extract_text_from_pdf(pdf_path)
                with open(pdf_path, "rb") as f:
                    pdf_bytes = f.read()
                logger.info(
                    f"Extração concluída, texto com {len(text)} caracteres. "
                    f"Tamanho do PDF em bytes: {len(pdf_bytes)}."
                )

                # Mover o PDF para a pasta 'PdfLidos'
                moved_pdf_path = move_pdf_to_lidos(pdf_path)

            except Exception as e:
                logger.error(f"Erro ao ler '{pdf_path}': {e}")
                text = ""
                pdf_bytes = b""
                moved_pdf_path = move_pdf_to_error(pdf_path)  # Mover o PDF para a pasta de erros
                continue  # Pula para o próximo PDF do batch

            # Monta doc_data com binário, texto e o caminho do PDF movido
            doc_data = {
                "filename": os.path.basename(pdf_path),
                "text": text,
                "pdf_bytes": pdf_bytes,
                "moved_pdf_path": moved_pdf_path
            }
            doc_data_list.append(doc_data)

        # 4. Submetendo as tarefas ao cluster para os PDFs no lote
        logger.info("Submetendo tarefas de processamento para o lote...")
        futures = client.map(process_document_with_ai, doc_data_list)

        # 5. Esperar os resultados do lote
        logger.info("Aguardando resultados dos workers...")
        results = client.gather(futures)

        # 6. Exibir / salvar o resultado do lote
        for r in results:
            logger.info(f"Resultado para {r['filename']}: {r}")

            extracted_json = r.get("extracted_json")
            if extracted_json:
                logger.info(
                    f"\n===== JSON Extraído para {r['filename']} =====\n"
                    f"{extracted_json}\n"
                    "============================================\n"
                )

                # Salvar o JSON extraído em um arquivo
                extracted_json_path = os.path.join("results", r['filename'] + "_extracted.json")
                try:
                    with open(extracted_json_path, "w", encoding='utf-8') as f:
                        f.write(extracted_json)
                    logger.info(f"JSON extraído salvo em: {extracted_json_path}")
                except Exception as e:
                    logger.error(f"Falha ao salvar JSON extraído em '{extracted_json_path}': {e}")

                # Gerar o script SQL
                contrato = ContractParser.parse(extracted_json)
                sql_script = generate_sql_script(contrato, r['filename'])
                
                # Opcionalmente, salvar também o script em arquivo .sql
                sql_script_path = os.path.join("results", r['filename'] + "_script.sql")
                try:
                    with open(sql_script_path, "w", encoding='utf-8') as f:
                        f.write(sql_script)
                    logger.info(f"Script SQL gerado e salvo em: {sql_script_path}")
                except Exception as e:
                    logger.error(f"Falha ao salvar script SQL em '{sql_script_path}': {e}")

                # *** Parte nova: executar automaticamente o script no banco ***
                try:
                    execute_sql_script(sql_script)
                    logger.info("Script SQL executado com sucesso no banco!")
                except Exception as e:
                    logger.error(f"Erro ao executar script SQL no banco: {e}")

            # Salva o dicionário de "result" (status final) em um JSON local
            local_res_path = os.path.join("results", r['filename'] + ".json")
            try:
                with open(local_res_path, 'w', encoding='utf-8') as f:
                    json.dump(r, f, ensure_ascii=False, indent=2)
                logger.info(f"Resultado salvo em: {local_res_path}")
            except Exception as e:
                logger.error(f"Falha ao salvar resultado em '{local_res_path}': {e}")

    # 7. Encerrar cluster
    logger.info("Encerrando client e cluster.")
    client.close()
    cluster.close()
    logger.info("Master Orchestrator finalizado.")


if __name__ == "__main__":
    main()
