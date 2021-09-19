import os
import logging

import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from models.models import Cidade, Estado, Mesorregiao, Microrregiao, CnaeSecao, CnaeDivisao, CnaeGrupo, CnaeClasse, Cnae, MotivoSituacaoCadastral, NaturezaJuridica, SituacaoCadastral, Porte, Cnpj, deleta, inicia
from utils.funcoes import criaEngine, seeder

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sessao = Session(criaEngine())

DIR = os.path.abspath('') + '/seeders'


def criaDados(lista: list, pasta: str = None):
    """Função que insere os dados no banco de dados.
    A função insere tanto dados em CSV quanto dados em formato de DataFrame.
    O parametro `pasta` indica se os dados vem em um DataFrame (sem pasta) ou em um csv (com pasta).

    Os dados devem ser passados dentro da `lista`, seguindo um dos formatos abaixo:
    * Para arquivos CSV:
    [('nome_do_arquivo1.csv', Model1), ('nome_do_arquivo2.csv', Model2), ... ('nome_do_arquivoN.csv', ModelN)]

    * Para DataFrames:
    [('nome de exibição1', pd.DataFrame1, Model1), ... ('nome de exibiçãoN', pd.DataFrameN, ModelN)]

    O nome de exibição é apenas para indicar no retorno.

    Args:
        lista (list): Lista com as tuplas contendo as duas possíveis estruturas explicadas acima.
        pasta (str, optional): Indica a pasta onde estão os arquivos. Defaults to None.

    Returns:
        Retorno [Dict]: Dicionario com o nome do arquivo/de exibição como chave e o status da inserção como valor.
    """
    retorno = {}
    if not pasta:  # significa que estou passando dataframes e não arquivos.
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
                seeder(sessao, pasta, arquivo, model)
                retorno[f'{arquivo[:-4]}'] = 'Inserido com sucesso!'
            except IntegrityError:
                sessao.rollback()
                retorno[f'{arquivo[:-4]}'] = 'Falha! Provavelmente os dados já estão inseridos.'

    return retorno


def postGeral():
    """Função que insere os dados de:
    * Cidades;
    * Cnaes;
    * Motivos de situação cadastral;
    * Naturezas jurídicas;
    * Situação Cadastral;
    * Portes.

    Os dados estão disponíveis em ./seeders

    Returns:
        Retorno[Dict]: Dicionario com o nome do arquivo/de exibição como chave e o status da inserção como valor.
    """
    # Apaga toda a base de dados
    deleta()
    # Inicia o banco de dados novamente, criando as tabelas
    inicia()

    cidades = [('estados.csv', Estado), ('Mesorregiões.csv', Mesorregiao), ("Microrregiões.csv", Microrregiao), ('Cidades.csv', Cidade)]
    cnaes = [('secoes.csv', CnaeSecao), ('divisoes.csv', CnaeDivisao), ("grupos.csv", CnaeGrupo), ('classes.csv', CnaeClasse), ('subclasses.csv', Cnae)]
    motivo_natureza = [('motivos.csv', MotivoSituacaoCadastral), ('naturezas.csv', NaturezaJuridica)]

    pasta_cidades = DIR + '/cidades'
    pasta_cnaes = DIR + '/cnaes'
    pasta_cnpjs = DIR + '/cnpjs'

    # Cria as cidades:
    retorno = criaDados(cidades, pasta_cidades)
    # Cria as CNAES:
    retorno.update(criaDados(cnaes, pasta_cnaes))
    # Cria motivos e naturezas:
    retorno.update(criaDados(motivo_natureza, pasta_cnpjs))

    # Existem 2 DFs que crio manualmente:
    situacao_cadastral = pd.DataFrame({'id': [1, 2, 3, 4, 8], 'descricao': ['Nula', 'Ativa', 'Suspensa', 'Inapta', 'Baixada']})
    porte = pd.DataFrame({'codigo': [0, 1, 3, 5], 'descricao': ['Não Informado', 'Micro Empresa', 'Empresa de Pequeno Porte', 'Demais']})

    # Crio a situação e o porte
    situacao_porte = [("sitCadastral", situacao_cadastral, SituacaoCadastral), ("Porte", porte, Porte)]
    retorno.update(criaDados(situacao_porte))
    return retorno


def postCnpjs():
    """Funçao que insere os dados de CNPJs no banco de dados.

    Para fazer a inserção, a função espera que exista um arquivo csv chamado `cnpjs` com os dados na pasta `./seeders/cnpjs/`.

    Por limitações da análise que escolhi realizar, não estou inserindo todas as colunas no banco de dados.
    Além disso, estou tratando apenas os dados do estado do Paraná, de empresas ativas ou que fecharam de 01/01/2016 em diante.
    Para mais informações sobre os arquivos de input e das limitações, leia o README.md
    """
    arquivo = DIR + "/cnpjs/cnpjs.csv"

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
            chunk["data_situacao_cadastral"] = pd.to_datetime(chunk["data_situacao_cadastral"], errors='coerce')
            chunk = chunk.loc[chunk['uf'].str.upper() == 'PR'].reset_index(drop=True)
            chunk = chunk.loc[(chunk['situacao_cadastral'] == 2) | (chunk['data_situacao_cadastral'] >= pd.to_datetime('2016-01-01'))].reset_index(drop=True)
            chunk['matriz'] = chunk['identificador_matriz_filial'] == 1
            chunk['mei'] = chunk['opcao_pelo_mei'] == "S"
            chunk['cep'] = chunk['cep'].astype(str).fillna('').apply(lambda x: x[:8])
            chunk = chunk.drop(['identificador_matriz_filial', 'opcao_pelo_mei', 'uf'], axis=1).rename(columns=renomear).fillna("NULL")
            chunk['cnae_id'] = chunk['cnae_id'].replace(5812300, 5812301)
            chunk['cnae_id'] = chunk['cnae_id'].replace(4751200, 4751201)
            chunk['cnae_id'] = chunk['cnae_id'].replace(1091100, 1091101)
            if len(chunk) > 0:
                try:
                    seeder(sessao=sessao, model=Cnpj, df=chunk)
                except IntegrityError as e:
                    logging.error(e)
                    sessao.rollback()
                    break


if __name__ == '__main__':
    retorno = postGeral()
    for k, v in retorno:
        logging.info(f"Inserção dados de {k} => {v}")
    postCnpjs()
