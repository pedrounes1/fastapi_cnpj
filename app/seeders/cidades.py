import os
import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session
from models.models import Cidade, Estado, Mesorregiao, Microrregiao
from utils.funcoes import criaEngine

sessao = Session(criaEngine())

DIR = os.path.dirname(__file__)


def p(arquivo: str):
    """Função para gerar o path (por isso p) para abrir e exportar os arquivos. `p` é mais curto que "`os.path.join(DIR, "../nome_do_arquivo"`

    Args:
        arquivo (str): nome do arquivo com extensão a ser aberto/salvo

    Returns:
        StrPath: Caminho completo para o arquivo, considerando a constante `DIR`.
    """
    return os.path.join(DIR, arquivo)


"""
Optei por transformar os dataframes em dicts para fazer a inserção ao invés de
inserir diretamente o df no banco de dados pois o pandas não possui upsert.
"""


def seeder(arquivo: str, model):
    dados = pd.read_csv(p(arquivo))
    if arquivo.startswith('estados'):
        dados = dados.rename(columns={'codigo_uf': 'id', 'nome': 'estado'})
    for item in dados.to_dict(orient='records'):
        reg = model(**item)
        sessao.add(reg)
    sessao.commit()


def seedGeral():
    retorno = {}
    for arquivo, model in [('estados.csv', Estado), ('Mesorregiões.csv', Mesorregiao), ("Microrregiões.csv", Microrregiao), ('Cidades.csv', Cidade)]:
        try:
            seeder(arquivo, model)
            retorno[f'{arquivo[:-4]}'] = 'Inserido com sucesso!'
        except IntegrityError:
            sessao.rollback()
            retorno[f'{arquivo[:-4]}'] = 'Falha! Provavelmente os dados já estão inseridos.'

    return retorno
