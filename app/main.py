from fastapi import FastAPI
from models.models import inicia, deleta

app = FastAPI()


@app.get('/')
def read_root():
    return({"msg": "Olá Mundo!"})


@app.post('/init_tables/{password}')
def criaTabelas(password: str):
    if password != "Arronbydo!":
        return {"erro": "Senha incorreta cuza1", "senha": password}
    else:
        inicia()
        return {'msg': 'Tabelas criadas com sucesso!'}


@app.post('/drop_tables/{password}')
def dropaTabelas(password: str):
    if password != "Arronbydo!":
        return {"erro": "Senha incorreta cuza1", "senha": password}
    else:
        deleta()
        return {'msg': 'Foi pro vinagre!'}
