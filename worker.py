# worker.py
import os
import json
import time
import boto3
import psycopg2
import logging
import sshtunnel
import tempfile
from botocore.exceptions import NoCredentialsError
import google.generativeai as genai
import concurrent.futures
from datetime import datetime

# Importar as classes do repo.py
from repo import ContractParser, ContractRepository

# Criar diretório de logs
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f'logs/worker_{os.getpid()}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Worker")

# Diretório temporário para processamento
TEMP_DIR = "/tmp/pdf_processing"
os.makedirs(TEMP_DIR, exist_ok=True)

# Config global para o worker
CONFIG = {
    'aws': {
        'ssh_host': 'ec2-54-175-112-114.compute-1.amazonaws.com',
        'ssh_username': 'ec2-user',
        'ssh_key_path': './sinerji.pem',
        'remote_db_host': 'localhost',
        'remote_db_port': 5432,
    },
    'database': {
        'dbname': 'compras_ia',
        'user': 'compras',
        'password': '12345'
    },
    's3': {
        'bucket': 'seu-bucket-s3',
        'region': 'us-east-1'
    },
    'gemini': {
        'api_key': 'AIzaSyAykyWUpxP0KUh_JhLtYMKl0gXq1IzzWBY',
        'model': 'gemini-pro',
        'timeout': 120
    }
}


class AWSConnector:
    """Gerencia conexões com a AWS (SSH, S3, banco de dados)."""
    def __init__(self, config):
        self.config = config
        self.tunnel = None
        
    def open_ssh_tunnel(self):
        """Abre túnel SSH para o banco de dados."""
        try:
            self.tunnel = sshtunnel.SSHTunnelForwarder(
                (self.config['aws']['ssh_host'], 22),
                ssh_username=self.config['aws']['ssh_username'],
                ssh_pkey=self.config['aws']['ssh_key_path'],
                remote_bind_address=(
                    self.config['aws']['remote_db_host'],
                    self.config['aws']['remote_db_port']
                ),
                local_bind_address=('localhost', 5434)  # Porta local
            )
            self.tunnel.start()
            return True
        except Exception as e:
            logger.error(f"Erro ao abrir túnel SSH: {e}")
            return False
    
    def connect_to_database(self):
        """Conecta ao banco de dados através do túnel SSH."""
        if not self.tunnel or not self.tunnel.is_active:
            if not self.open_ssh_tunnel():
                return None
                
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=self.tunnel.local_bind_port,
                dbname=self.config['database']['dbname'],
                user=self.config['database']['user'],
                password=self.config['database']['password'],
                connect_timeout=30
            )
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco via túnel SSH: {e}")
            return None
    
    def upload_to_s3(self, content, s3_key):
        """Faz upload de conteúdo para o S3 com re-tentativas."""
        max_attempts = 3
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            try:
                s3_client = boto3.client('s3', region_name=self.config['s3']['region'])
                
                if isinstance(content, str):
                    s3_client.put_object(
                        Bucket=self.config['s3']['bucket'],
                        Key=s3_key,
                        Body=content.encode('utf-8'),
                        ContentType='application/json'
                    )
                else:
                    s3_client.put_object(
                        Bucket=self.config['s3']['bucket'],
                        Key=s3_key,
                        Body=content
                    )
                return True
            except NoCredentialsError:
                logger.error("Credenciais da AWS não encontradas.")
                return False
            except Exception as e:
                if attempt == max_attempts:
                    logger.error(f"Erro ao fazer upload para S3 após {max_attempts} tentativas: {e}")
                    return False
                else:
                    logger.warning(f"Tentativa {attempt} falhou. Tentando novamente em 5s... Erro: {e}")
                    time.sleep(5)
    
    def close(self):
        """Fecha o túnel SSH se estiver ativo."""
        if self.tunnel and self.tunnel.is_active:
            self.tunnel.stop()


def analyze_with_gemini(self, text):
    """
    Analisa o texto do PDF usando a API Gemini, agora com o prompt que
    extrai as informações de contrato e gera o JSON.
    O model_name será obtido de self.model_entry (entry Tkinter), e 
    o timeout, da CONFIG (ou onde você preferir).
    """
    start_time = time.time()
    
    try:
        # Configurar credencial da API
        genai.configure(api_key=CONFIG['gemini']['api_key'])
        
        # Escolher o modelo a partir do que o usuário digitou na GUI
        model = genai.GenerativeModel(
            model_name=self.model_entry.get().strip(),
            generation_config={
                "temperature": 0,
                "top_p": 0.95,
                "top_k": 40
            }
        )
        
        # Se o texto for muito grande, truncar para 30000 caracteres
        if len(text) > 30000:
            logger.info(f"Texto muito longo ({len(text)} chars), truncando para 30000.")
            text = text[:30000]
        
        # Novo prompt
        prompt = f"""Você é um especialista em análise de contratos públicos. Sua tarefa é extrair as informações essenciais de um contrato ou termo administrativo de forma precisa e estruturada.
        Analise o documento a seguir e gere um JSON rigorosamente no seguinte formato (sem comentários ou texto adicional):

{{
  "numero_contrato": "XXXXXXXXX",
  "tipo_instrumento": "Contrato",
  "processo_administrativo": "XXXXXXXXX",
  "data_celebracao": "DD/MM/AAAA",
  "orgao_contratante": {{
    "razao_social": "Nome do Órgão",
    "sigla": "SIGLA_DO_ÓRGÃO",
    "cnpj": "XX.XXX.XXX/XXXX-XX"
  }},
  "empresa_contratada": {{
    "razao_social": "Nome da Empresa",
    "cnpj": "XX.XXX.XXX/XXXX-XX"
  }},
  "itens": [
    {{
      "descricao": "Nome do Item",
      "especificacao": "Detalhes Técnicos",
      "unidade_medida": "Unidade de Fornecimento (ex: unidade, litro, metro)",
      "quantidade": "Número total de unidades adquiridas",
      "valor_unitario": "R$ X,XX",
      "valor_total": "R$ X.XXX,XX",
      "catmat_catser": "Código CATMAT/CATSER",
      "tipo": "Material",  
      "locais_execucao_entrega": "Cidade (UF), Cidade (UF)"
    }}
  ],
  "fonte_preco": "Contrato",
  "referencia_contrato": "Número do contrato de onde os preços foram extraídos",
  "anexo_contrato": "Caminho ou link do arquivo do contrato",
  "status_extracao": "Sucesso"
}}

Regras para Extração:
- *Número do contrato*: localizar no cabeçalho do documento.
- *Tipo de instrumento*: se for um contrato, informar "Contrato"; caso contrário, extrair o nome exato.
- *Data de celebração*: se o documento for gerado via SEI, buscar a data no final; caso contrário, extrair do cabeçalho ou das cláusulas iniciais.
- *Órgão contratante*: extrair a razão social, a sigla (se existir ou puder ser deduzida do texto) e o CNPJ do órgão responsável.
- *Empresa contratada*: extrair a razão social e o CNPJ da empresa contratada.
- *Itens*:
   • Descrição e especificação técnica;  
   • Unidade de medida;  
   • Quantidade adquirida;  
   • Valor unitário e valor total;  
   • Código CATMAT/CATSER (se disponível);  
   • Tipo: indicar "Material" ou "Serviço";  
   • Locais de execução/entrega: retornar no formato *Cidade (UF)*. Se houver mais de um local, utilize vírgula para separá-los.
- *Fonte do preço*: sempre "Contrato".
- *Referência do contrato*: repetir o número do contrato utilizado.
- *Anexo do contrato*: incluir o caminho ou link de acesso.
- *Status da extração*: "Sucesso" se todas as informações forem extraídas, "Parcial" se faltar alguma, ou "Erro" se a extração falhar.

*Texto para análise:*
{text}
"""

        timeout = CONFIG['gemini']['timeout']
        
        # Usar um executor para tratar timeout
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(model.generate_content, prompt)
            try:
                response = future.result(timeout=timeout)
                response_text = response.text
                duration = time.time() - start_time
                
                # Tentar converter a resposta para JSON diretamente
                try:
                    json.loads(response_text)
                    logger.info(f"Análise Gemini concluída em {duration:.2f}s")
                    return response_text, True
                except json.JSONDecodeError:
                    # Se não for JSON válido, tentar extrair
                    logger.error("Resultado não é um JSON válido.")
                    json_start = response_text.find('{')
                    json_end = response_text.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        json_only = response_text[json_start:json_end]
                        try:
                            json.loads(json_only)
                            logger.warning("JSON extraído do texto de resposta.")
                            return json_only, True
                        except:
                            pass
                    
                    # Se chegou aqui, não deu para parsear como JSON
                    snippet = response_text[:100].replace('\n', ' ')
                    return f"Resultado não é um JSON válido: {snippet}...", False
                
            except concurrent.futures.TimeoutError:
                logger.error(f"Timeout na chamada API Gemini após {timeout}s")
                return "Timeout na análise com IA", False
                
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Erro ao analisar com Gemini após {duration:.2f}s: {e}")
        return str(e), False

def process_document_with_ai(doc_data):
    """
    Fluxo principal executado pelos workers:
    1. Recebe texto extraído do PDF
    2. Processa com Gemini
    3. Salva no banco de dados via SSH
    4. Faz upload do JSON para S3
    """
    start_time = time.time()
    filename = doc_data['filename']
    result = {
        'filename': filename,
        'success': False,
        'processing_time': 0,
        'error': None,
        'steps_completed': []
    }
    
    logger.info(f"Processando documento: {filename}")
    aws = None
    conn = None
    
    try:
        # 1. Analisar texto com Gemini
        text_length = len(doc_data['text'])
        logger.info(f"Enviando {text_length} caracteres para análise")
        json_str, success = analyze_with_gemini(doc_data['text'])
        result['steps_completed'].append('ai_analysis')
        
        if not success:
            result['error'] = f"Falha na análise com Gemini: {json_str}"
            return result
        
        # 2. Armazenar temporariamente o JSON
        with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=TEMP_DIR, suffix='.json') as temp_file:
            temp_json_path = temp_file.name
            temp_file.write(json_str)
        result['steps_completed'].append('json_saved')
        
        # 3. Converter JSON para objeto Contrato
        try:
            contrato = ContractParser.parse(json_str)
            result['steps_completed'].append('json_parsed')
        except Exception as e:
            result['error'] = f"Erro ao parsear JSON: {e}"
            logger.error(f"JSON inválido retornado: {json_str[:500]}...")
            return result
        
        # 4. Abrir conexão SSH e com banco
        aws = AWSConnector(CONFIG)
        conn = aws.connect_to_database()
        
        if not conn:
            result['error'] = "Falha na conexão com o banco de dados"
            return result
        
        result['steps_completed'].append('db_connected')
        
        # 5. Persistir no banco
        repo = ContractRepository(conn)
        repo.persist_contract(contrato)
        result['steps_completed'].append('db_persisted')
        
        # 6. Upload do JSON para S3
        s3_key = f"contratos_analisados/{os.path.splitext(filename)[0]}.json"
        s3_success = aws.upload_to_s3(json_str, s3_key)
        if s3_success:
            result['steps_completed'].append('s3_json_uploaded')
        
        # 7. Upload do PDF original para S3 (se disponível)
        pdf_path = doc_data.get('path')
        if pdf_path and os.path.exists(pdf_path):
            with open(pdf_path, 'rb') as pdf_file:
                pdf_content = pdf_file.read()
                s3_pdf_key = f"contratos_originais/{filename}"
                pdf_s3_success = aws.upload_to_s3(pdf_content, s3_pdf_key)
                if pdf_s3_success:
                    result['steps_completed'].append('s3_pdf_uploaded')
        
        result['success'] = True
        result['s3_upload'] = s3_success
        result['processing_time'] = time.time() - start_time
        logger.info(f"Documento {filename} processado com sucesso em {result['processing_time']:.2f}s")
        
    except Exception as e:
        result['error'] = str(e)
        result['processing_time'] = time.time() - start_time
        logger.error(f"Erro ao processar {filename}: {e}")
        
    finally:
        # Fechamento
        try:
            if conn:
                conn.close()
            if aws:
                aws.close()
            if 'temp_json_path' in locals() and os.path.exists(temp_json_path):
                os.unlink(temp_json_path)
        except Exception as close_error:
            logger.warning(f"Erro ao fechar recursos: {close_error}")
        
    return result
