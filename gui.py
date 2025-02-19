# gui.py
import os
import threading
import traceback
import tkinter as tk
from tkinter import filedialog, messagebox

import psycopg2

# Importa as classes e métodos do repo.py
from repo import ContractParser, ContractRepository


class ExtratorGUI:
    """
    Interface gráfica para:
     - Configurar dados de conexão
     - Selecionar diretório de arquivos JSON
     - Processar os arquivos em Thread (sem travar a UI)
     - Exibir logs
    """

    def __init__(self, master):
        self.master = master
        self.master.title("Extrator de Contratos (com Threads)")
        self.master.geometry("600x400")

        # Frame para dados de conexão
        frame_db = tk.LabelFrame(master, text="Configuração de Banco de Dados")
        frame_db.pack(fill="x", padx=10, pady=5)

        tk.Label(frame_db, text="Host:").grid(row=0, column=0, sticky="e")
        tk.Label(frame_db, text="Port:").grid(row=1, column=0, sticky="e")
        tk.Label(frame_db, text="DB Name:").grid(row=2, column=0, sticky="e")
        tk.Label(frame_db, text="User:").grid(row=0, column=2, sticky="e")
        tk.Label(frame_db, text="Password:").grid(row=1, column=2, sticky="e")

        self.host_var = tk.StringVar(value="localhost")
        self.port_var = tk.StringVar(value="5432")
        self.dbname_var = tk.StringVar(value="seu_banco")
        self.user_var = tk.StringVar(value="seu_usuario")
        self.pass_var = tk.StringVar(value="sua_senha")

        tk.Entry(frame_db, textvariable=self.host_var, width=15).grid(row=0, column=1, padx=5, pady=2)
        tk.Entry(frame_db, textvariable=self.port_var, width=5).grid(row=1, column=1, padx=5, pady=2)
        tk.Entry(frame_db, textvariable=self.dbname_var, width=10).grid(row=2, column=1, padx=5, pady=2)
        tk.Entry(frame_db, textvariable=self.user_var, width=10).grid(row=0, column=3, padx=5, pady=2)
        tk.Entry(frame_db, textvariable=self.pass_var, show="*", width=10).grid(row=1, column=3, padx=5, pady=2)

        # Frame para diretório JSON
        frame_dir = tk.LabelFrame(master, text="Diretório de Arquivos JSON")
        frame_dir.pack(fill="x", padx=10, pady=5)

        self.dir_var = tk.StringVar()
        tk.Entry(frame_dir, textvariable=self.dir_var, width=50).grid(row=0, column=0, padx=5, pady=2)
        tk.Button(frame_dir, text="Selecionar Pasta", command=self.select_directory).grid(row=0, column=1, padx=5, pady=2)

        # Botão de processamento
        frame_buttons = tk.Frame(master)
        frame_buttons.pack(fill="x", padx=10, pady=5)

        self.process_button = tk.Button(frame_buttons, text="Processar Contratos", command=self.start_processing_thread)
        self.process_button.pack(side="left")

        # Campo de saída (logs)
        frame_output = tk.LabelFrame(master, text="Log do Processamento")
        frame_output.pack(fill="both", expand=True, padx=10, pady=5)

        self.txt_output = tk.Text(frame_output, wrap="word")
        self.txt_output.pack(fill="both", expand=True)

    def select_directory(self):
        """Seleciona pasta de arquivos JSON."""
        selected_dir = filedialog.askdirectory(title="Selecione o Diretório")
        if selected_dir:
            self.dir_var.set(selected_dir)

    def start_processing_thread(self):
        """
        Inicia a thread para processar contratos
        (assim a GUI não fica travada).
        """
        dir_path = self.dir_var.get().strip()
        if not os.path.isdir(dir_path):
            messagebox.showerror("Erro", "Diretório inválido!")
            return

        # Pega dados de conexão
        host = self.host_var.get().strip()
        port = self.port_var.get().strip()
        dbname = self.dbname_var.get().strip()
        user = self.user_var.get().strip()
        password = self.pass_var.get().strip()

        # Desabilita o botão para evitar cliques duplos
        self.process_button.config(state=tk.DISABLED)

        # Cria a thread de processamento
        thread = threading.Thread(
            target=self.process_contracts_in_thread,
            args=(host, port, dbname, user, password, dir_path),
            daemon=True  # se a GUI fechar, encerra a thread
        )
        thread.start()

    def process_contracts_in_thread(self, host: str, port: str, dbname: str,
                                    user: str, password: str, dir_path: str):
        """
        Executado em background para processar arquivos JSON sem travar a GUI.
        """
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password
            )
        except Exception:
            self.safe_log_output(f"[ERRO] Falha na conexão com o BD:\n{traceback.format_exc()}")
            self.enable_process_button()
            return

        repo = ContractRepository(conn)

        processed_count = 0
        error_count = 0

        for filename in os.listdir(dir_path):
            if filename.lower().endswith(".json"):
                full_path = os.path.join(dir_path, filename)
                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        json_content = f.read()

                    contrato = ContractParser.parse(json_content)
                    repo.persist_contract(contrato)

                    processed_count += 1
                    self.safe_log_output(f"[OK] {filename} processado com sucesso.")
                except Exception:
                    error_count += 1
                    err_msg = traceback.format_exc()
                    self.safe_log_output(f"[ERRO] Falha ao processar '{filename}':\n{err_msg}")

        conn.close()
        self.safe_log_output(f"\nConcluído. Sucessos: {processed_count}, Erros: {error_count}")
        self.enable_process_button()

    def safe_log_output(self, msg: str):
        """
        Adiciona texto ao campo de log de forma segura,
        usando 'after' para atualizar na thread principal.
        """
        def _append_text():
            self.txt_output.insert(tk.END, msg + "\n")
            self.txt_output.see(tk.END)

        self.master.after(0, _append_text)

    def enable_process_button(self):
        """Reabilita o botão 'Processar Contratos'."""
        def _enable():
            self.process_button.config(state=tk.NORMAL)

        self.master.after(0, _enable)


def main():
    root = tk.Tk()
    app = ExtratorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
