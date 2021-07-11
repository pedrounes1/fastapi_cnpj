import pandas as pd
from unidecode import unidecode

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

engine = ''


def criaEngine(echo: bool = False):
    global engine
    if engine == '':
        print("To criando a engine!")
        engine = create_engine('postgresql://postgres:entralogo@pg:5432/cnpj', future=True, echo=echo)
    else:
        print("Já tenho a engine!")
    return engine


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


def consultaDados(consulta, as_df=True, echo=False, processar=True):
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
