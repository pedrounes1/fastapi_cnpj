from fastapi import FastAPI, HTTPException
from typing import Optional
from views.cidades import getCidades, getMeso, getMicro, getEstados
from views.cnaes import getClasses, getDivisoes, getSecoes, getGrupos, getSubclasses

"""
* criar o parametro detalhes: True | False. Detalhes busca os nomes das regiões relacionadas.
* Faixa DDD?
"""
app = FastAPI()


@app.get('/')
def root_app():
    return [{'tipo': 'Dados Geográficos',
             'urls': ['/geo/estados/', '/geo/mesorregioes/', '/geo/microrregioes/', '/geo/cidades/']}]


@app.get('/geo/')
def root_geo():
    return getEstados()


@app.get('/geo/estados/')
def rota_estados(id: Optional[int] = None, listar: Optional[str] = None):
    if listar:
        listar = str(listar).lower()
    estados = getEstados(id)
    if not estados or len(estados) == 0:
        raise HTTPException(status_code=404, detail='Estado não encontrado!')
    for estado in estados:
        if listar == 'mesorregioes':
            estado['mesorregioes'] = getMeso(estado_id=estado['estado_id'])
        elif listar == 'microrregioes':
            estado['microrregioes'] = getMicro(parent_id=estado['estado_id'], parent='estado')
        elif listar == 'cidades':
            estado['cidades'] = getCidades(parent_id = estado['estado_id'], parent='estado')

    return estados


@app.get('/geo/mesorregioes/')
def rota_mesorregioes(id: Optional[int] = None, estado_id: Optional[int] = None, listar: Optional[str] = None):
    mesos = getMeso(id, estado_id)
    if not mesos or len(mesos) == 0:
        raise HTTPException(status_code=404, detail='Mesorregião não encontrada!')
    if listar:
        listar = str(listar).lower()
    if listar == 'microrregioes':
        for meso in mesos:
            meso['microrregioes'] = getMicro(parent_id=meso['meso_id'], parent='mesorregiao')
    if listar == 'cidades':
        for meso in mesos:
            meso['cidades'] = getCidades(parent_id=meso['meso_id'], parent='mesorregiao')
    return mesos


@app.get('/geo/microrregioes/')
def rota_microrregioes(id: Optional[int] = None, listar: Optional[str] = None):
    micros = getMicro(id)
    if listar:
        listar = str(listar).lower()
    if not micros or len(micros) == 0:
        raise HTTPException(status_code=404, detail='Microrregião não encontrado!')
    if listar == 'cidades':
        for micro in micros:
            micro['cidades'] = getCidades(parent_id=micro['micro_id'], parent='microrregiao')
    return micros


@app.get('/geo/cidades/')
def rota_cidades(id: Optional[int] = None, ddd: Optional[str] = None):
    cidades = getCidades(ddd, id)
    if not cidades or len(cidades) == 0:
        raise HTTPException(status_code=404, detail='Cidade não encontrada')
    return cidades


@app.get('/cnaes/secoes/')
def rota_secoes(id: Optional[str] = None, listar: Optional[str] = None):
    secoes =  getSecoes(id)
    listar = listar.lower() if listar else None
    for secao in secoes:
        if listar == 'divisoes':
            secao['divisoes'] = ['abuble','is','a','good','placeholder']
        elif listar == 'grupos':
            secao['grupos'] = ['abuble','is','a','good','placeholder']
        elif listar == 'classes':
            secao['classes'] = ['abuble','is','a','good','placeholder']
        elif listar == 'subclasses':
            secao['subclasses'] = ['abuble','is','a','good','placeholder']
    return secoes
    # raise HTTPException(status_code=501, detail='Função não implementada!')


@app.get('/cnaes/divisoes/')
def rota_divisoes(id: Optional[str] = None, listar: Optional[str] = None):
    divisoes =  getDivisoes(id)
    listar = listar.lower() if listar else None
    for divisao in divisoes:
        if listar == 'grupos':
            divisao['grupos'] = ['abuble','is','a','good','placeholder']
        elif listar == 'classes':
            divisao['classes'] = ['abuble','is','a','good','placeholder']
        elif listar == 'subclasses':
            divisao['subclasses'] = ['abuble','is','a','good','placeholder']
    return divisoes


@app.get('/cnaes/grupos/')
def rota_grupos(id: Optional[str] = None, listar: Optional[str] = None):
    grupos =  getGrupos(id)
    listar = listar.lower() if listar else None
    for grupo in grupos:
        if listar == 'classes':
            grupo['classes'] = ['abuble','is','a','good','placeholder']
        elif listar == 'subclasses':
            grupo['subclasses'] = ['abuble','is','a','good','placeholder']
    return grupos


@app.get('/cnaes/classes/')
def rota_classes(id: Optional[str] = None, listar: Optional[str] = None):
    classes =  getClasses(id)
    listar = listar.lower() if listar else None
    for classe in classes:
        if listar == 'subclasses':
            classe['subclasses'] = ['abuble','is','a','good','placeholder']
    return classes


@app.get('/cnaes/cnaes/')
def rota_cnaes(id: Optional[str] = None, secao: Optional[str] = None, divisao: Optional[int] = None, grupo: Optional[int] = None, classe: Optional[int] = None):
    if secao:
        retorno = getSubclasses(parent='secao',parent_id=secao.upper())
    elif divisao:
        retorno = getSubclasses(parent='divisao',parent_id=divisao)
    elif grupo:
        retorno = getSubclasses(parent='grupo',parent_id=grupo)
    elif classe:
        retorno = getSubclasses(parent='classe',parent_id=classe)
    else:
        retorno = getSubclasses(id)
    return retorno
