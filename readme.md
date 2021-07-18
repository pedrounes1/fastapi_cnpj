# FastAPI CNPJ
## Container para o backend da análise de CNPJs

Este projeto tem como objetivo montar um container Docker para processar o backend de análises dos dados de CNPJ do estado do Paraná, usando [FastAPI](https://fastapi.tiangolo.com/) e [PostgreSQL](https://www.postgresql.org/). Esse container usa como base a imagem do docker **tiangolo/uvicorn-gunicorn-fastapi** e depende de um container **postges**. as dependências estão listadas no requirements.txt

