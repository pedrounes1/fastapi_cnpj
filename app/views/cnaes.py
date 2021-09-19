from utils.funcoes import consultaDados
from sqlalchemy import select, func, distinct

from models.models import CnaeSecao, CnaeDivisao, CnaeGrupo, CnaeClasse, Cnae


def contagem(obj, nome):
    if nome == 'secoes':
        res = select(
            func.count(distinct(CnaeDivisao.id)).label('qtde_divisoes'),
            func.count(distinct(CnaeGrupo.id)).label('qtde_grupos'),
            func.count(distinct(CnaeClasse.id)).label('qtde_classes'),
            func.count(distinct(Cnae.id)).label('qtde_subclasses')) \
            .select_from(CnaeSecao) \
            .join(CnaeSecao.divisoes) \
            .join(CnaeDivisao.grupos) \
            .join(CnaeGrupo.classes) \
            .join(CnaeClasse.cnaes) \
            .where(CnaeSecao.id == obj['id']) \
            .group_by(CnaeSecao.id)
        res = consultaDados(res, False)[0]
        obj.update(res)
    if nome == 'divisoes':
        res = select(
            func.count(distinct(CnaeGrupo.id)).label('qtde_grupos'),
            func.count(distinct(CnaeClasse.id)).label('qtde_classes'),
            func.count(distinct(Cnae.id)).label('qtde_subclasses')) \
            .select_from(CnaeDivisao) \
            .join(CnaeDivisao.grupos) \
            .join(CnaeGrupo.classes) \
            .join(CnaeClasse.cnaes) \
            .where(CnaeDivisao.id == obj['id']) \
            .group_by(CnaeDivisao.id)
        res = consultaDados(res, False)[0]
        obj.update(res)
    if nome == 'grupos':
        res = select(
            func.count(distinct(CnaeClasse.id)).label('qtde_classes'),
            func.count(distinct(Cnae.id)).label('qtde_subclasses')) \
            .select_from(CnaeGrupo) \
            .join(CnaeGrupo.classes) \
            .join(CnaeClasse.cnaes) \
            .where(CnaeGrupo.id == obj['id']) \
            .group_by(CnaeGrupo.id)
        res = consultaDados(res, False)
        if len(res) == 0:
            res = [{'qtde_classes':0, 'qtde_subclasses':0}]

        obj.update(res[0])
    if nome == 'classes':
        res = select(
            func.count(distinct(Cnae.id)).label('qtde_subclasses')) \
            .where(Cnae.classe_id == obj['id'])
        res = consultaDados(res, False)[0]
        obj.update(res)
    return obj


def getSecoes(id: str = None):
    dados = select(
        CnaeSecao.id,
        CnaeSecao.descricao)
    if id:
        dados = dados.where(CnaeSecao.id == id.upper())
    return [contagem(secao, 'secoes') for secao in consultaDados(dados, False)]


def getDivisoes(id: int, secao_id: str = None):
    dados = select(
        CnaeDivisao.id,
        CnaeDivisao.descricao)
    if id:
        dados = dados.where(CnaeDivisao.id == id)
    elif secao_id:
        dados - dados.where(CnaeDivisao.secao_id == secao_id.upper())
    return [contagem(divisao, 'divisoes') for divisao in consultaDados(dados, False)]


'''
Secao => Divisão => Grupo => Classe => Subclasse
'''

def getGrupos(id:int = None, parent: str = None, parent_id: str or int = None):
    dados = select(
        CnaeGrupo.id,
        CnaeGrupo.descricao)
    if id:
        dados = dados.where(CnaeGrupo.id == id)
    if parent == 'divisao':
        dados = dados.where(CnaeGrupo.divisao_id == parent_id)
    elif parent == 'secao':
        dados = dados.select_from(CnaeSecao).join(CnaeSecao.divisoes).join(CnaeDivisao.grupos).where(CnaeSecao.id == parent_id.upper())
    return [contagem(grupo, 'grupos') for grupo in consultaDados(dados, False)]


def getClasses(id:int = None, parent: str = None, parent_id: str or int = None):
    dados = select(
        CnaeClasse.id,
        CnaeClasse.descricao)
    if id:
        dados = dados.where(CnaeClasse.id == id)

    if parent == 'grupo':
        dados = dados.where(CnaeClasse.grupo_id == parent_id)
    elif parent == 'divisao':
        dados = dados.select_from(CnaeDivisao).join(CnaeDivisao.grupos).join(CnaeGrupo.classes).where(CnaeDivisao.id == parent_id)
    elif parent == 'secao':
        dados = dados.select_from(CnaeSecao).join(CnaeSecao.divisoes).join(CnaeDivisao.grupos).join(CnaeGrupo.classes).where(CnaeSecao.id == parent_id.upper())
    return [contagem(classe, 'classes') for classe in consultaDados(dados, False)]

def getSubclasses(id:int = None, parent: str = None, parent_id: str or int = None):
    dados = select(Cnae)
    if id:
        dados = dados.where(Cnae.id == id)

    if parent == 'classe':
        dados = dados.where(Cnae.classe_id == parent_id)
    if parent == 'grupo':
        dados = dados.select_from(CnaeGrupo).join(CnaeGrupo.classes).join(CnaeClasse.cnaes).where(CnaeGrupo.id == parent_id)
    elif parent == 'divisao':
        dados = dados.select_from(CnaeDivisao).join(CnaeDivisao.grupos).join(CnaeGrupo.classes).join(CnaeClasse.cnaes).where(CnaeDivisao.id == parent_id)
    elif parent == 'secao':
        dados = dados.select_from(CnaeSecao).join(CnaeSecao.divisoes).join(CnaeDivisao.grupos).join(CnaeGrupo.classes).join(CnaeClasse.cnaes).where(CnaeSecao.id == parent_id.upper())
    return consultaDados(dados, False)