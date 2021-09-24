import os
import logging

import pandas as pd
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.session import Session

from models.models import Cidade, Estado, Mesorregiao, Microrregiao, CnaeSecao, CnaeDivisao, CnaeGrupo, CnaeClasse, Cnae, MotivoSituacaoCadastral, NaturezaJuridica, SituacaoCadastral, Porte, Cnpj, deleta, inicia
from utils.funcoes import consultaDados, criaEngine, seeder

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
    motivo_natureza = [(DIR + 'motivos.csv', MotivoSituacaoCadastral),
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

    with pd.read_csv(arquivo, encoding='utf-8', sep='#', chunksize=150_000, usecols=cols, low_memory=False) as reader:
        for cont, chunk in enumerate(reader):
            logging.info(f'Inserção CNPJs: Processando o chunk {cont}...')
            chunk = chunk.loc[(chunk['uf'].str.upper() != 'EX') & (~chunk['codigo_municipio'].isna())].reset_index(drop=True)
            chunk["data_situacao_cadastral"] = pd.to_datetime(chunk["data_situacao_cadastral"], errors='coerce')
            chunk = chunk.loc[(chunk['situacao_cadastral'] == 2) | (chunk['data_situacao_cadastral'] >= pd.to_datetime('2013-01-01'))].reset_index(drop=True)
            chunk['matriz'] = chunk['identificador_matriz_filial'] == 1
            chunk['mei'] = chunk['opcao_pelo_mei'] == "S"
            chunk['cep'] = chunk['cep'].astype(str).fillna('').apply(lambda x: x[:8])
            chunk = chunk.drop(['identificador_matriz_filial', 'opcao_pelo_mei', 'uf'], axis=1).rename(columns=renomear).fillna("NULL")
            chunk['natureza_id'] = chunk['natureza_id'].replace(2100, 2062)
            chunk['cnae_id'] = chunk['cnae_id'].replace({5812300: 5812301, 4751200: 4751201, 1091100: 1091101, 3511500: 3511501,
                                                         8888888: 8299799, 4721101: 4721102, 2539000: 2539001, 5611202: 5611204, 1822900: 1822999})

            if len(chunk) > 0:
                try:
                    seeder(sessao=sessao, model=Cnpj, df=chunk, logging=logging)
                    logging.info('dados inseridos!')
                except IntegrityError as e:
                    logging.error(e)
                    sessao.rollback()
                    break
            del chunk


def testaCnaes():
    arquivo = DIR + "cnpjs.csv"
    cols = ['codigo_municipio', 'data_situacao_cadastral', 'situacao_cadastral', 'uf']
    cnaes = set()
    municipios = set()
    naturezas = set()

    with pd.read_csv(arquivo, encoding='utf-8', sep='#', chunksize=100_000, usecols=cols) as reader:
        for cont, chunk in enumerate(reader):
            logging.info(f'Inserção CNPJs: Processando o chunk {cont}...')
            chunk = chunk.loc[chunk['uf'].str.upper() != 'EX'].reset_index(drop=True)
            chunk["data_situacao_cadastral"] = pd.to_datetime(chunk["data_situacao_cadastral"], errors='coerce')
            chunk = chunk.loc[(chunk['situacao_cadastral'] == 2) | (chunk['data_situacao_cadastral'] >= pd.to_datetime('2013-01-01'))].reset_index(drop=True)

            cnaes = cnaes.union(chunk['cnae_fiscal'].unique())
            municipios = municipios.union(chunk['codigo_municipio'].unique())
            naturezas = naturezas.union(chunk['codigo_natureza_juridica'].unique())

            del chunk

    logging.info("Todos os chunks foram processados!")

    municipios_bd = municipios.difference(set(consultaDados(select(Cidade.siafi_id))['siafi_id']))
    cnaes_bd = cnaes.difference(set(consultaDados(select(Cnae.id))['id']))
    naturezas_bd = naturezas.difference(set(consultaDados(select(NaturezaJuridica.id))['id']))

    logging.info('Cidades não cadastradas:')
    logging.info(municipios_bd)

    logging.info('Naturezas não cadastradas:')
    logging.info(naturezas_bd)

    logging.info('Cnaes não cadastradas:')
    logging.info(cnaes_bd)

    with open('./dados ausentes.txt', 'w', encoding='utf-8') as fl:
        fl.write('Cidades: \n')
        fl.writelines([str(x) + "\n" for x in municipios_bd])
        fl.write('CNAES: \n')
        fl.writelines([str(x) + "\n" for x in cnaes_bd])
        fl.write('Naturezas: \n')
        fl.writelines([str(x) + "\n" for x in naturezas_bd])


if __name__ == '__main__':
    retorno = postGeral()
    for k, v in retorno.items():
        logging.info(f"Inserção dados de {k} => {v}")
    postCnpjs()
