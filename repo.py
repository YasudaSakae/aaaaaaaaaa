# repo.py
import json
import logging
from datetime import datetime
from typing import List, Optional

# Caso use psycopg2, o import deve estar aqui ou no chamador
import psycopg2


# Configuração básica de logging
# (Você pode customizar melhor, criando handlers e formatters)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# =========================================
# ============= DOMAIN MODELS ============
# =========================================

class OrgaoContratante:
    def __init__(self, razao_social: str, sigla: Optional[str], cnpj: str):
        self.razao_social = razao_social
        self.sigla = sigla
        self.cnpj = cnpj


class EmpresaContratada:
    def __init__(self, razao_social: str, cnpj: str):
        self.razao_social = razao_social
        self.cnpj = cnpj


class Item:
    def __init__(self,
                 descricao: str,
                 especificacao: Optional[str],
                 unidade_medida: Optional[str],
                 quantidade: Optional[str],
                 valor_unitario: Optional[str],
                 valor_total: Optional[str],
                 catmat_catser: Optional[str],
                 tipo: str,
                 locais_execucao_entrega: Optional[str]):
        self.descricao = descricao
        self.especificacao = especificacao
        self.unidade_medida = unidade_medida
        self.quantidade = quantidade
        self.valor_unitario = valor_unitario
        self.valor_total = valor_total
        self.catmat_catser = catmat_catser
        self.tipo = tipo
        self.locais_execucao_entrega = locais_execucao_entrega


class Contrato:
    def __init__(self,
                 numero_contrato: str,
                 tipo_instrumento: str,
                 processo_administrativo: Optional[str],
                 data_celebracao: Optional[str],
                 fonte_preco: str,
                 referencia_contrato: Optional[str],
                 anexo_contrato: Optional[str],
                 status_extracao: str,
                 orgao_contratante: OrgaoContratante,
                 empresa_contratada: EmpresaContratada,
                 itens: List[Item]):
        self.numero_contrato = numero_contrato
        self.tipo_instrumento = tipo_instrumento
        self.processo_administrativo = processo_administrativo
        self.data_celebracao = data_celebracao
        self.fonte_preco = fonte_preco
        self.referencia_contrato = referencia_contrato
        self.anexo_contrato = anexo_contrato
        self.status_extracao = status_extracao
        self.orgao_contratante = orgao_contratante
        self.empresa_contratada = empresa_contratada
        self.itens = itens


# =========================================
# ============= PARSER CLASS =============
# =========================================

class ContractParser:
    """
    Responsável por converter JSON em objetos de domínio.
    """
    @staticmethod
    def parse(json_str: str) -> Contrato:
        data = json.loads(json_str)

        oc_data = data.get("orgao_contratante", {})
        orgao_contratante = OrgaoContratante(
            razao_social=oc_data.get("razao_social", ""),
            sigla=oc_data.get("sigla"),
            cnpj=oc_data.get("cnpj", "")
        )

        ec_data = data.get("empresa_contratada", {})
        empresa_contratada = EmpresaContratada(
            razao_social=ec_data.get("razao_social", ""),
            cnpj=ec_data.get("cnpj", "")
        )

        itens_list = []
        for item_data in data.get("itens", []):
            item = Item(
                descricao=item_data.get("descricao", ""),
                especificacao=item_data.get("especificacao"),
                unidade_medida=item_data.get("unidade_medida"),
                quantidade=item_data.get("quantidade"),
                valor_unitario=item_data.get("valor_unitario"),
                valor_total=item_data.get("valor_total"),
                catmat_catser=item_data.get("catmat_catser"),
                tipo=item_data.get("tipo", "Material"),
                locais_execucao_entrega=item_data.get("locais_execucao_entrega")
            )
            itens_list.append(item)

        contrato = Contrato(
            numero_contrato=data.get("numero_contrato", ""),
            tipo_instrumento=data.get("tipo_instrumento", "Contrato"),
            processo_administrativo=data.get("processo_administrativo"),
            data_celebracao=data.get("data_celebracao"),
            fonte_preco=data.get("fonte_preco", "Contrato"),
            referencia_contrato=data.get("referencia_contrato"),
            anexo_contrato=data.get("anexo_contrato"),
            status_extracao=data.get("status_extracao", "Sucesso"),
            orgao_contratante=orgao_contratante,
            empresa_contratada=empresa_contratada,
            itens=itens_list
        )
        return contrato


# =========================================
# ========== REPOSITORY CLASS =============
# =========================================

class ContractRepository:
    """
    Classe responsável por fazer a persistência no Postgres (schema extrator).
    Inclui logging e tratamento de exceções.
    """

    def __init__(self, conn):
        self.conn = conn
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_orgao_contratante_by_cnpj(self, cnpj: str) -> Optional[int]:
        sql = """
            SELECT id FROM extrator.ext_orgao_contratante
             WHERE cnpj = %s
             LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (cnpj,))
            row = cur.fetchone()
            return row[0] if row else None

    def get_empresa_contratada_by_cnpj(self, cnpj: str) -> Optional[int]:
        sql = """
            SELECT id FROM extrator.ext_empresa_contratada
             WHERE cnpj = %s
             LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (cnpj,))
            row = cur.fetchone()
            return row[0] if row else None

    def insert_orgao_contratante(self, orgao: OrgaoContratante) -> int:
        existing_id = self.get_orgao_contratante_by_cnpj(orgao.cnpj)
        if existing_id is not None:
            self.logger.debug(f"Órgão já existe (ID={existing_id}).")
            return existing_id

        sql = """
            INSERT INTO extrator.ext_orgao_contratante (
                razao_social, sigla, cnpj
            ) VALUES (%s, %s, %s)
            RETURNING id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (orgao.razao_social, orgao.sigla, orgao.cnpj))
            new_id = cur.fetchone()[0]
            self.logger.debug(f"Órgão inserido com ID={new_id}.")
            return new_id

    def insert_empresa_contratada(self, empresa: EmpresaContratada) -> int:
        existing_id = self.get_empresa_contratada_by_cnpj(empresa.cnpj)
        if existing_id is not None:
            self.logger.debug(f"Empresa já existe (ID={existing_id}).")
            return existing_id

        sql = """
            INSERT INTO extrator.ext_empresa_contratada (
                razao_social, cnpj
            ) VALUES (%s, %s)
            RETURNING id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (empresa.razao_social, empresa.cnpj))
            new_id = cur.fetchone()[0]
            self.logger.debug(f"Empresa inserida com ID={new_id}.")
            return new_id

    def insert_contrato(self, contrato: Contrato,
                        orgao_id: int,
                        empresa_id: int) -> int:
        data_celebracao_sql = None
        if contrato.data_celebracao:
            try:
                data_celebracao_sql = datetime.strptime(
                    contrato.data_celebracao, "%d/%m/%Y"
                ).date()
            except ValueError:
                self.logger.warning(
                    f"Data de celebração inválida: {contrato.data_celebracao}"
                )
                data_celebracao_sql = None

        sql = """
            INSERT INTO extrator.ext_contratos (
                numero_contrato,
                tipo_instrumento,
                processo_administrativo,
                data_celebracao,
                fonte_preco,
                referencia_contrato,
                anexo_contrato,
                status_extracao,
                orgao_contratante_id,
                empresa_contratada_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                contrato.numero_contrato,
                contrato.tipo_instrumento,
                contrato.processo_administrativo,
                data_celebracao_sql,
                contrato.fonte_preco,
                contrato.referencia_contrato,
                contrato.anexo_contrato,
                contrato.status_extracao,
                orgao_id,
                empresa_id
            ))
            new_id = cur.fetchone()[0]
            self.logger.debug(f"Contrato inserido com ID={new_id}.")
            return new_id

    def insert_item(self, item: Item, contrato_id: int):
        sql = """
            INSERT INTO extrator.ext_itens (
                contrato_id,
                descricao,
                especificacao,
                unidade_medida,
                quantidade,
                valor_unitario,
                valor_total,
                catmat_catser,
                tipo,
                locais_execucao_entrega
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                contrato_id,
                item.descricao,
                item.especificacao,
                item.unidade_medida,
                item.quantidade,
                item.valor_unitario,
                item.valor_total,
                item.catmat_catser,
                item.tipo,
                item.locais_execucao_entrega
            ))

    def insert_log_extrator(self,
                            cnpj_orgao: str,
                            cnpj_empresa: str,
                            numero_contrato: str,
                            data_contrato: Optional[datetime.date],
                            quantidade_itens: int,
                            status_execucao: str,
                            mensagem_log: str):
        sql = """
            INSERT INTO extrator.ext_log_extrator (
                data_registro,
                cnpj_orgao,
                cnpj_empresa,
                numero_contrato,
                data_contrato,
                quantidade_itens,
                status_execucao,
                mensagem_log
            )
            VALUES (now(), %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                cnpj_orgao,
                cnpj_empresa,
                numero_contrato,
                data_contrato,
                quantidade_itens,
                status_execucao,
                mensagem_log
            ))

    def persist_contract(self, contrato: Contrato):
        """
        Orquestra a inserção do contrato e itens em transação.
        Gera log em extrator.ext_log_extrator no final.
        """
        self.logger.info(f"Processando contrato: {contrato.numero_contrato}")
        log_status = "Sucesso"
        log_msg = "Contrato processado com sucesso."
        data_contrato_sql = None

        if contrato.data_celebracao:
            try:
                data_contrato_sql = datetime.strptime(
                    contrato.data_celebracao, "%d/%m/%Y"
                ).date()
            except ValueError:
                data_contrato_sql = None

        qtd_itens = len(contrato.itens)

        try:
            self.conn.autocommit = False

            orgao_id = self.insert_orgao_contratante(contrato.orgao_contratante)
            empresa_id = self.insert_empresa_contratada(contrato.empresa_contratada)
            contrato_id = self.insert_contrato(contrato, orgao_id, empresa_id)

            for item in contrato.itens:
                self.insert_item(item, contrato_id)

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            log_status = "Falha"
            log_msg = f"Erro ao processar contrato: {e}"
            self.logger.error(f"Falha no processamento do contrato {contrato.numero_contrato}.", exc_info=True)
        finally:
            self.conn.autocommit = True
            try:
                self.insert_log_extrator(
                    cnpj_orgao=contrato.orgao_contratante.cnpj,
                    cnpj_empresa=contrato.empresa_contratada.cnpj,
                    numero_contrato=contrato.numero_contrato,
                    data_contrato=data_contrato_sql,
                    quantidade_itens=qtd_itens,
                    status_execucao=log_status,
                    mensagem_log=log_msg
                )
            except Exception as e_log:
                self.logger.error("Falha ao registrar log do extrator.", exc_info=True)

