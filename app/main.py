from fastapi import FastAPI, HTTPException
from typing import Optional
from starlette.requests import Request
from models.models import inicia, deleta
from seeders.cidades import seedGeral
from views.cidades import getCidadesLista, getCidade, getMesoLista, getMeso, getMicroLista, getMicro, getEstados

app = FastAPI()


@app.get('/')
def read_root():
    return getEstados()


@app.get('/estados/')
def estados(id: Optional[int] = None, listar: Optional[str] = None):
    print(id, listar)
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


@app.get('/mesorregioes/')
def mesorregioes(id: Optional[int], listar: Optional[str]):
    meso = getMeso(id)
    if not meso or len(meso) == 0:
        raise HTTPException(status_code=404, detail='Mesorregião não encontrada!')
    if listar == 'microrregioes':
        meso['microrregioes'] = getMicroLista(id, 'mesorregiao')
    if listar == 'cidades':
        meso['cidades'] = getCidadesLista(id, 'mesorregiao')
    return meso


@app.get('/microrregioes/')
def microrregioes(id: Optional[int], listar: Optional[str]):
    micro = getMicro(id)
    if not micro or len(micro) == 0:
        raise HTTPException(status_code=404, detail='Microrregião não encontrado!')
    if listar == 'cidades':
        micro['cidades'] = getCidadesLista(id, 'microrregiao')
    return micro


@app.get('/cidades/')
def cidades(id: Optional[int]):
    cidade = getCidade(id)
    if not cidade or len(cidade) == 0:
        raise HTTPException(status_code=404, detail='Cidade não encontrada')
    return cidade


@app.post('/tables/')
async def trataTables(req: Request):
    args = await req.json()
    if args['op'] == 'inicia':
        inicia()
        seedGeral()
        return {'msg': 'Tabelas criadas e dados inseridos com sucesso!'}
    elif args['op'] == 'deleta':
        deleta()
        return {'msg': 'Já era!'}
