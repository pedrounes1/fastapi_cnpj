from fastapi import FastAPI, HTTPException
from typing import Optional


from views.cidades import getCidadesLista, getCidade, getMesoLista, getMeso, getMicroLista, getMicro, getEstados


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
def estados(id: Optional[int] = None, listar: Optional[str] = None):
    if listar:
        listar = str(listar).lower()
    estado = ''
    if not id:
        return getEstados()
    else:
        estado = getEstados(id)

    if not estado or len(estado) == 0:
        raise HTTPException(status_code=404, detail='Estado não encontrado!')
    if listar == 'mesorregioes':
        estado['mesorregioes'] = getMesoLista(id)
    elif listar == 'microrregioes':
        estado['microrregioes'] = getMicroLista(id, 'estado')
    elif listar == 'cidades':
        estado['cidades'] = getCidadesLista(id, 'estado')

    return estado


@app.get('/geo/mesorregioes/')
def mesorregioes(id: Optional[int] = None, listar: Optional[str] = None):
    meso = getMeso(id)
    if listar:
        listar = str(listar).lower()
    if not meso or len(meso) == 0:
        raise HTTPException(status_code=404, detail='Mesorregião não encontrada!')
    if listar == 'microrregioes':
        meso['microrregioes'] = getMicroLista(id, 'mesorregiao')
    if listar == 'cidades':
        meso['cidades'] = getCidadesLista(id, 'mesorregiao')
    return meso


@app.get('/geo/microrregioes/')
def microrregioes(id: Optional[int] = None, listar: Optional[str] = None):
    if id:
        micro = [getMicro(id)]
    else:
        micro = getMicroLista()
    if listar:
        listar = str(listar).lower()
    if not micro or len(micro) == 0:
        raise HTTPException(status_code=404, detail='Microrregião não encontrado!')
    if listar == 'cidades':
        for m in micro:
            m['cidades'] = getCidadesLista(id, 'microrregiao')
    return micro


@app.get('/geo/cidades/')
def cidades(id: Optional[int] = None, ddd: Optional[int] = None):
    if id:
        cidade = getCidade(id)
    elif ddd:
        cidade = getCidade(ddd)
    else:
        cidade = getCidadesLista()
    if not cidade or len(cidade) == 0:
        raise HTTPException(status_code=404, detail='Cidade não encontrada')
    return cidade


@app.get('/cnaes/sessoes/')
def sessoes(id: Optional[str] = None, listar: Optional[str] = None):
    raise HTTPException(status_code=501, detail='Função não implementada!')


@app.get('/cnaes/divisoes/')
def divisoes(id: Optional[str] = None, listar: Optional[str] = None):
    raise HTTPException(status_code=501, detail='Função não implementada!')


@app.get('/cnaes/grupos/')
def grupos(id: Optional[str] = None, listar: Optional[str] = None):
    raise HTTPException(status_code=501, detail='Função não implementada!')


@app.get('/cnaes/classes/')
def classes(id: Optional[str] = None, listar: Optional[str] = None):
    raise HTTPException(status_code=501, detail='Função não implementada!')


@app.get('/cnaes/cnaes/')
def cnaes(id: Optional[str] = None, listar: Optional[str] = None):
    raise HTTPException(status_code=501, detail='Função não implementada!')
