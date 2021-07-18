from sqlalchemy.sql.functions import count
from models.models import Cidade, Estado, Mesorregiao, Microrregiao
from sqlalchemy import select
from utils.funcoes import consultaDados


def contagem(obj, nome):
    def cnt(crit_1, crit_2, crit_3):
        return consultaDados(select(count(crit_1)).where(crit_2 == crit_3), False)[0]['count']
    if nome == 'estado':
        obj['mesorregioes_count'] = cnt(Mesorregiao.id, Mesorregiao.estado_id, obj['estado_id'])
        obj['microrregioes_count'] = cnt(Microrregiao.id, Microrregiao.estado_id, obj['estado_id'])
        obj['cidades_count'] = cnt(Cidade.id, Cidade.estado_id, obj['estado_id'])

    elif nome == 'mesorregiao':
        obj['cidades_count'] = cnt(Cidade.id, Cidade.meso_id, obj['meso_id'])
        obj['microrregioes_count'] = cnt(Microrregiao.id, Microrregiao.meso_id, obj['meso_id'])
    elif nome == 'microrregiao':
        obj['cidades_count'] = cnt(Cidade.id, Cidade.micro_id, obj['micro_id'])
    return obj


def getEstados(id=None):
    if id:
        retorno = consultaDados(select(Estado).where(Estado.id == id), False)
        if len(retorno) == 0:
            return
        retorno = contagem(retorno[0], 'estado')
    else:
        retorno = consultaDados(select(Estado.id.label('estado_id'), Estado.estado.label('nome')), False)
        for item in retorno:
            item = contagem(item, 'estado')
    return retorno


def getMesoLista(estado_id: int):
    return consultaDados(select(Mesorregiao.id, Mesorregiao.nome).where(Mesorregiao.estado_id == estado_id).order_by(Mesorregiao.nome), False)


def getMeso(id: int):
    dados = consultaDados(select(Mesorregiao).where(Mesorregiao.id == id), False)
    if len(dados) == 0:
        return
    return contagem(dados[0], 'mesorregiao')


def getMicroLista(parent_id: int, parent: str):
    consulta = select(Microrregiao.id, Microrregiao.nome).order_by(Microrregiao.nome)
    if parent.lower() == 'estado':
        consulta = consulta.where(Microrregiao.estado_id == parent_id)
    elif parent.lower() == 'mesorregiao':
        consulta = consulta.where(Microrregiao.meso_id == parent_id)
    return consultaDados(consulta, False)


def getMicro(id: int):
    dados = consultaDados(select(Microrregiao).where(Microrregiao.id == id), False)
    if len(dados) == 0:
        return
    return contagem(dados[0], 'microrregiao')


def getCidadesLista(parent_id: int, parent: str):
    consulta = select(Cidade.id, Cidade.nome).order_by(Cidade.nome)
    if parent.lower() == 'estado':
        consulta = consulta.where(Cidade.estado_id == parent_id)
    elif parent.lower() == 'mesorregiao':
        consulta = consulta.where(Cidade.meso_id == parent_id)
    elif parent.lower() == 'microrregiao':
        consulta = consulta.where(Cidade.micro_id == parent_id)
    return consultaDados(consulta, False)


def getCidade(id: int):
    dados = consultaDados(select(Cidade).where(Cidade.id == id), False)
    if len(dados) == 0:
        return
    return dados
