from sqlalchemy.sql.functions import count
from models.models import Cidade, Estado, Mesorregiao, Microrregiao
from sqlalchemy import select
from sqlalchemy.exc import ProgrammingError
from utils.funcoes import consultaDados
from fastapi import HTTPException


def contagem(obj, nome):
    def cnt(crit_1, crit_2, crit_3):
        return consultaDados(select(count(crit_1)).where(crit_2 == crit_3), False)[0]['count']
    if nome == 'estado':
        obj['mesorregioes_qtde'] = cnt(Mesorregiao.id, Mesorregiao.estado_id, obj['estado_id'])
        obj['microrregioes_qtde'] = cnt(Microrregiao.id, Microrregiao.estado_id, obj['estado_id'])
        obj['cidades_qtde'] = cnt(Cidade.id, Cidade.estado_id, obj['estado_id'])

    elif nome == 'mesorregiao':
        obj['cidades_qtde'] = cnt(Cidade.id, Cidade.meso_id, obj['meso_id'])
        obj['microrregioes_qtde'] = cnt(Microrregiao.id, Microrregiao.meso_id, obj['meso_id'])
    elif nome == 'microrregiao':
        obj['cidades_qtde'] = cnt(Cidade.id, Cidade.micro_id, obj['micro_id'])
    return obj


def getEstados(id=None):
    dados  = select(
        Estado.id.label('estado_id'),
        Estado.estado.label('nome'),
        Estado.uf,
        Estado.latitude,
        Estado.longitude)
    if id:
        dados = dados.where(Estado.id == id)
    retorno = consultaDados(dados, False)

    return [contagem(x, 'estado') for x in retorno]

def getMeso(id: int, estado_id: int = None):
    dados = select(Mesorregiao)
    if id:
        dados = dados.where(Mesorregiao.id == id)
    elif estado_id:
        dados = dados.where(Mesorregiao.estado_id == estado_id).order_by(Mesorregiao.nome)

    return [contagem(meso, "mesorregiao") for meso in consultaDados(dados, False)]


def getMicroLista(parent_id: int = None, parent: str = None):
    consulta = select(Microrregiao).order_by(Microrregiao.nome)
    if parent:
        if parent.lower() == 'estado':
            consulta = consulta.where(Microrregiao.estado_id == parent_id)
        elif parent.lower() == 'mesorregiao':
            consulta = consulta.where(Microrregiao.meso_id == parent_id)
    return consultaDados(consulta, False)


def getMicro(id: int = None, parent: str = None, parent_id: int = None):
    dados = select(Microrregiao).order_by(Microrregiao.nome)
    if id:
        dados = dados.where(Microrregiao.id == id)
    if parent == 'estado':
        dados = dados.where(Microrregiao.estado_id == parent_id)
    elif parent == 'mesorregiao':
        dados = dados.where(Microrregiao.meso_id == parent_id)
    return [contagem(micro, 'microrregiao') for micro in consultaDados(dados, False)]


def getCidades(ddd: str = None, id: int = None, parent: str = None, parent_id: int = None):
    dados = select(Cidade).order_by(Cidade.nome)
    if id:
        dados = dados.where(Cidade.id == id)
    elif ddd:
        dados = dados.where(Cidade.ddd == ddd)

    if parent == 'estado':
        dados = dados.where(Cidade.estado_id == parent_id)
    elif parent == 'mesorregiao':
        dados = dados.where(Cidade.meso_id == parent_id)
    return consultaDados(dados, False)


def getCidadesLista(parent_id: int = None, parent: str = None):
    consulta = select(Cidade).order_by(Cidade.nome)
    if parent:
        if parent.lower() == 'estado':
            consulta = consulta.where(Cidade.estado_id == parent_id)
        elif parent.lower() == 'mesorregiao':
            consulta = consulta.where(Cidade.meso_id == parent_id)
        elif parent.lower() == 'microrregiao':
            consulta = consulta.where(Cidade.micro_id == parent_id)
    return consultaDados(consulta, False)


def getCidade(param: int):
    if param > 99:
        dados = consultaDados(select(Cidade).where(Cidade.id == param), False)
    else:
        dados = consultaDados(select(Cidade.id, Cidade.nome, Cidade.estado_id).where(Cidade.ddd == str(param)), False)
        dados.insert(0, {'qtd': len(dados)})

    if len(dados) == 0:
        return
    return dados
