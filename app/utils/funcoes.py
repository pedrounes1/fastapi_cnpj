import os
import pandas as pd
from unidecode import unidecode

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

engine = ''

DOCKER = os.getenv("DOCKER")


def p(pasta, arquivo: str):
    """Função para gerar o path (por isso p) para abrir e exportar os arquivos. `p` é mais curto que "`os.path.join(DIR, "../nome_do_arquivo"`

    Args:
        arquivo (str): nome do arquivo com extensão a ser aberto/salvo

    Returns:
        StrPath: Caminho completo para o arquivo, considerando a constante `DIR`.
    """
    return os.path.join(pasta, arquivo)


def seeder(sessao: Session, pasta: str = None, arquivo: str = None, model=None, df: pd.DataFrame = ""):
    """
    Optei por transformar os dataframes em dicts para fazer a inserção ao invés de
    inserir diretamente o df no banco de dados pois o pandas não possui upsert.

    Args:
        sessao ([type]): [description]
        pasta (str): [description]
        arquivo (str): [description]
        model ([type]): [description]
    """
    if len(df) == 0:
        dados = pd.read_csv(p(pasta, arquivo))
        if arquivo.startswith('estados'):
            dados = dados.rename(columns={'codigo_uf': 'id', 'nome': 'estado'})
    else:
        dados = df
    for item in dados.to_dict(orient='records'):
        for k, v in item.items():
            if str(v).lower() in ['nan', 'nat', 'none', 'null']:
                item[k] = None

        reg = model(**item)
        sessao.add(reg)
    sessao.commit()


def criaEngine(echo: bool = False):
    if DOCKER:
        return create_engine('postgresql://postgres:entralogo@pg:5432/cnpj', future=True, echo=echo)
    else:
        return create_engine('postgresql://postgres:entralogo@localhost:5432/cnpj', future=True, echo=echo)


def formataColuna(df, col):
    return df[col].astype("str").apply(lambda x: unidecode(x).upper().strip())


def validaArgs(args, params):
    for elm in set(params):
        if elm not in set(args.keys()):
            return False
    return True


def trataRegistro(registro):
    """Trata as classes do SQLAlchemy que retornam nas consultas.
    Essa função é chamada dentro de processaRespostas, para tratar o Campo.
    A função pega uma instancia de um Model criado e retorna um dicionário com o nome do atributo + prefixo do Model como chave e o valor do campo como valor
    Função auxiliar da função processaRespostas e, por consequencia, da função consultaDados.
    Args:
        registro (Model): retorno da consulta ao banco de dados.
    Returns:
        Dict: Registro da linha na forma de dicionário.
    """
    retorno = {f'{registro._prefixo}{k}': v for k, v in registro.__dict__.items()}
    if f'{registro._prefixo}_sa_instance_state' in retorno.keys():
        retorno.pop(f'{registro._prefixo}_sa_instance_state')
    return retorno


def processaResposta(res):
    """Trata a resposta da consulta realizada ao banco de dados.
    Uma consulta (ex.: Select()) retorna uma resposta.
    Uma resposta pode conter uma ou mais linhas.
    Cada linha pode possuir:
        > Uma instância de algum Model;
        > Um campo específico de uma coluna do banco de dados.
    Essa função processa a resposta e retorna uma lista com dicionários, para ser transformada em DataFrame.
    Função auxiliar a função consultaDados.
    Args:
        res (ChunkedIteratorResult): Resposta da consulta feita ao banco de dados.
    Returns:
        List[Dict]: Retorna a resposta tratada, tendo cada linha em um dicionario.
    """
    retorno = []
    for linha in res.mappings():
        ret = {}
        for k, v in linha.items():
            if '_sa_registry' in dir(v):  # Ou seja, é classe.
                ret.update(trataRegistro(v))
            else:  # Não é classe, é outra coisa
                ret[k] = v
        retorno.append(ret)
    return retorno


def consultaDados(consulta, as_df=True, processar=True):
    """Função usada para realizar consultas no banco de dados e tratar o retorno.
    Args:
        consulta (Select): Um Select do SQLAlchemy
        as_df (bool, optional): Retorna os dados como pandas.DataFrame (True) ou como List[Dict] (False). Defaults to True.
        echo (bool, optional): Parâmetro passado pro SQLAlchemy. Define se terá log da consulta. Defaults to False.
    Returns:
        DataFrame | List[Dict]: Retorno da consulta realizada ao banco de dados.
    """
    session = Session(engine)

    with session as s:
        res = s.execute(consulta)

    if processar:
        res = processaResposta(res)

    if as_df:
        return pd.DataFrame(res)

    return res
