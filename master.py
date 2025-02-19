# master_orchestrator.py
import glob
import os
from dask.distributed import Client
from setup_cluster import setup_master_scheduler  # se for subir localmente

from pdf_analyzer import extract_text_from_pdf
from worker import process_document_with_ai

def main():
    # 1. Subir o cluster local (master + scheduler) ou conectar num scheduler remoto
    #    Se você já estiver iniciando "python setup_cluster.py master" via terminal,
    #    pode simplesmente fazer: client = Client("tcp://IP_DO_MASTER:8786")
    #    Mas aqui vou mostrar subindo localmente:
    cluster, client = setup_master_scheduler()

    # 2. Localizar PDFs no HD externo
    pdf_dir = "D:/HD_EXTERNO/contratos"  # Exemplo
    pdf_paths = glob.glob(os.path.join(pdf_dir, "*.pdf"))

    if not pdf_paths:
        print("Nenhum PDF encontrado em", pdf_dir)
        return

    # 3. Extrair texto de cada PDF (localmente, pois só o master enxerga o HD)
    doc_data_list = []
    for pdf_path in pdf_paths:
        text = extract_text_from_pdf(pdf_path)
        doc_data = {
            "filename": os.path.basename(pdf_path),
            "text": text,
            "path": pdf_path
            # Adicionar mais campos se for preciso
        }
        doc_data_list.append(doc_data)

    # 4. Submeter essas tarefas (uma por PDF) para os workers
    #    Cada worker chama process_document_with_ai(...)
    futures = client.map(process_document_with_ai, doc_data_list)

    # 5. Esperar resultados
    results = client.gather(futures)

    # 6. Exibir / Logar
    for r in results:
        print(r)

    # 7. Encerrar cluster
    client.close()
    cluster.close()

if __name__ == "__main__":
    main()
