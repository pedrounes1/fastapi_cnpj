import os
import logging

import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from models.models import Cidade, Estado, Mesorregiao, Microrregiao, CnaeSecao, CnaeDivisao, CnaeGrupo, CnaeClasse, Cnae, MotivoSituacaoCadastral, NaturezaJuridica, SituacaoCadastral, Porte, Cnpj, deleta, inicia
from utils.funcoes import criaEngine, seeder

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sessao = Session(criaEngine())

DIR = os.path.abspath('') + '/data/cnpjs/'


def criaDados(lista: list, as_df: bool = False):
    """Função que insere os dados no banco de dados.
    A função insere tanto dados em CSV quanto dados em formato de DataFrame.
    O parametro `as_df` indica se os dados vem em um DataFrame (True) ou em um csv (False).

    Os dados devem ser passados dentro da `lista`, seguindo um dos formatos abaixo:
    * Para arquivos CSV:
    [('url/ou/caminho/do/arquivo1.csv', Model1), ('url/ou/caminho/do/arquivo2.csv', Model2), ... ('url/ou/caminho/do/arquivoN.csv', ModelN)]

    * Para DataFrames:
    [('nome de exibição1', pd.DataFrame1, Model1), ... ('nome de exibiçãoN', pd.DataFrameN, ModelN)]

    O nome de exibição é apenas para indicar no retorno.

    Args:
        lista (list): Lista com as tuplas contendo as duas possíveis estruturas explicadas acima.
        as_df (bool, optional): Indica se os dados estão em um DataFrame. Defaults to False.

    Returns:
        Retorno [Dict]: Dicionario com o nome do arquivo/de exibição como chave e o status da inserção como valor.
    """
    retorno = {}
    if as_df:  # significa que estou passando dataframes e não arquivos.
        for nome, df, model in lista:
            try:
                seeder(sessao=sessao, model=model, df=df)
                retorno[nome] = 'Inserido com sucesso!'
            except IntegrityError:
                sessao.rollback()
                retorno[nome] = 'Falha! Provavelmente os dados já estão inseridos.'
    else:
        for arquivo, model in lista:
            try:
                seeder(sessao, arquivo, model)
                retorno[f'{arquivo.split("/")[-1][:-4]}'] = 'Inserido com sucesso!'
            except IntegrityError:
                sessao.rollback()
                retorno[f'{arquivo.split("/")[-1][:-4]}'] = 'Falha! Provavelmente os dados já estão inseridos.'

    return retorno


def postGeral():
    """Função que insere os dados de:
    * Cidades;
    * Cnaes;
    * Motivos de situação cadastral;
    * Naturezas jurídicas;
    * Situação Cadastral;
    * Portes.
    Returns:
        Retorno[Dict]: Dicionario com o nome do arquivo/de exibição como chave e o status da inserção como valor.
    """
    # Apaga toda a base de dados
    deleta()
    # Inicia o banco de dados novamente, criando as tabelas
    inicia()
    urls = {
        "estados": "https://raw.githubusercontent.com/pedrounes1/Municipios-Brasileiros/main/csv/estados.csv",
        "mesorregioes": "https://raw.githubusercontent.com/pedrounes1/Municipios-Brasileiros/main/csv/mesorregioes.csv",
        "microrregioes": "https://raw.githubusercontent.com/pedrounes1/Municipios-Brasileiros/main/csv/microrregioes.csv",
        "cidades": "https://raw.githubusercontent.com/pedrounes1/Municipios-Brasileiros/main/csv/cidades.csv",
        "secoes": "https://raw.githubusercontent.com/pedrounes1/CNAES-IBGE-2_3/main/csv/secoes.csv",
        "divisoes": "https://raw.githubusercontent.com/pedrounes1/CNAES-IBGE-2_3/main/csv/divisoes.csv",
        "grupos": "https://raw.githubusercontent.com/pedrounes1/CNAES-IBGE-2_3/main/csv/grupos.csv",
        "classes": "https://raw.githubusercontent.com/pedrounes1/CNAES-IBGE-2_3/main/csv/classes.csv",
        "subclasses": "https://raw.githubusercontent.com/pedrounes1/CNAES-IBGE-2_3/main/csv/subclasses.csv"}

    cidades = [(urls['estados'], Estado), (urls['mesorregioes'], Mesorregiao),
               (urls['microrregioes'], Microrregiao), (urls['cidades'], Cidade)]
    cnaes = [(urls['secoes'], CnaeSecao), (urls['divisoes'], CnaeDivisao),
             (urls['grupos'], CnaeGrupo), (urls['classes'], CnaeClasse), (urls['subclasses'], Cnae)]
    motivo_natureza = [(DIR +'motivos.csv', MotivoSituacaoCadastral),
                       (DIR + 'naturezas.csv', NaturezaJuridica)]

    # Cria as cidades:
    retorno = criaDados(cidades)
    # Cria as CNAES:
    retorno.update(criaDados(cnaes))
    # Cria motivos e naturezas:
    retorno.update(criaDados(motivo_natureza))

    # Existem 2 DFs que crio manualmente:
    situacao_cadastral = pd.DataFrame({'id': [1, 2, 3, 4, 8], 'descricao': [
                                      'Nula', 'Ativa', 'Suspensa', 'Inapta', 'Baixada']})
    porte = pd.DataFrame({'codigo': [0, 1, 3, 5], 'descricao': [
                         'Não Informado', 'Micro Empresa', 'Empresa de Pequeno Porte', 'Demais']})

    # Crio a situação e o porte
    situacao_porte = [("sitCadastral", situacao_cadastral,
                       SituacaoCadastral), ("Porte", porte, Porte)]
    retorno.update(criaDados(situacao_porte, True))
    return retorno


def postCnpjs():
    """Funçao que insere os dados de CNPJs no banco de dados.

    Para fazer a inserção, a função espera que exista um arquivo csv chamado `cnpjs` com os dados na pasta `./seeders/cnpjs/`.

    Por limitações da análise que escolhi realizar, não estou inserindo todas as colunas no banco de dados.
    Além disso, estou tratando apenas os dados do estado do Paraná, de empresas ativas ou que fecharam de 01/01/2016 em diante.
    Para mais informações sobre os arquivos de input e das limitações, leia o README.md
    """
    arquivo = DIR + "cnpjs.csv"

    cols = ['cnpj', 'identificador_matriz_filial', 'razao_social', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
            'motivo_situacao_cadastral', 'codigo_natureza_juridica', 'data_inicio_atividade', 'cnae_fiscal', 'cep', 'codigo_municipio', 'uf',
            'porte_empresa', 'opcao_pelo_mei', 'situacao_especial', 'data_situacao_especial']

    renomear = {
        'situacao_cadastral': 'situacao_id',
        'motivo_situacao_cadastral': 'situacao_motivo_id',
        'codigo_natureza_juridica': 'natureza_id',
        'cnae_fiscal': 'cnae_id',
        'codigo_municipio': 'siafi_id',
        'porte_empresa': 'porte_id'}

    with pd.read_csv(arquivo, encoding='utf-8', sep='#', chunksize=50_000, usecols=cols, low_memory=False) as reader:
        for cont, chunk in enumerate(reader):
            logging.info(f'Inserção CNPJs: Processando o chunk {cont}...')
            chunk["data_situacao_cadastral"] = pd.to_datetime(
                chunk["data_situacao_cadastral"], errors='coerce')
            chunk = chunk.loc[chunk['uf'].str.upper() ==
                              'PR'].reset_index(drop=True)
            chunk = chunk.loc[(chunk['situacao_cadastral'] == 2) | (
                chunk['data_situacao_cadastral'] >= pd.to_datetime('2016-01-01'))].reset_index(drop=True)
            chunk['matriz'] = chunk['identificador_matriz_filial'] == 1
            chunk['mei'] = chunk['opcao_pelo_mei'] == "S"
            chunk['cep'] = chunk['cep'].astype(
                str).fillna('').apply(lambda x: x[:8])
            chunk = chunk.drop(['identificador_matriz_filial', 'opcao_pelo_mei', 'uf'], axis=1).rename(
                columns=renomear).fillna("NULL")
            chunk['natureza_id'] = chunk['natureza_id'].replace(2100, 2062)
            chunk['cnae_id'] = chunk['cnae_id'].replace(5812300, 5812301)
            chunk['cnae_id'] = chunk['cnae_id'].replace(4751200, 4751201)
            chunk['cnae_id'] = chunk['cnae_id'].replace(1091100, 1091101)
            chunk['cnae_id'] = chunk['cnae_id'].replace(8888888, 8299799)
            if len(chunk) > 0:
                try:
                    seeder(sessao=sessao, model=Cnpj, df=chunk)
                except IntegrityError as e:
                    logging.error(e)
                    sessao.rollback()
                    break
            del chunk


if __name__ == '__main__':
    retorno = postGeral()
    for k, v in retorno.items():
        logging.info(f"Inserção dados de {k} => {v}")
    postCnpjs()
