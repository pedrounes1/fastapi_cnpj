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
    try:
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
    except ProgrammingError:
        raise HTTPException(status_code=404, detail='Tabela não criada!')


def getMesoLista(estado_id: int):
    return consultaDados(select(Mesorregiao.id, Mesorregiao.nome).where(Mesorregiao.estado_id == estado_id).order_by(Mesorregiao.nome), False)


def getMeso(id: int):
    dados = select(Mesorregiao)
    if id:
        dados = dados.where(Mesorregiao.id == id)
    dados = consultaDados(dados, False)
    if len(dados) == 0:
        return

    retorno = []
    for meso in dados:
        retorno.append(contagem(meso, 'mesorregiao'))
    return retorno


def getMicroLista(parent_id: int = None, parent: str = None):
    consulta = select(Microrregiao).order_by(Microrregiao.nome)
    if parent:
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
