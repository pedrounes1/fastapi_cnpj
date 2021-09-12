# FastAPI CNPJ - Trabalho em progresso
Container para o backend da análise de CNPJs

## Contexto:
No fim de 2019, a Receita Federal disponibilizou os dados abertos de todos os CNPJs, ativos e inativos. Os dados incluem todas as informações disponíveis no cartão CNPJ, o que permite diversas análises. Os dados foram disponibilizados em um formato proprietário e o pessoal do Observatório Social do Brasil - Município de Santo Antônio de Jesus - Estado da Bahia tratou e disponibizou esses dados em formatos mais legíveis. Você pode encontrar os dados e as ferramentas para tratativas no [repositório deles no GitHub](https://github.com/georgevbsantiago/qsacnpj). Ao descobrir esses dados e, principalmente, esse repositório, decidi realizar minhas próprias análises exploratórias nos dados, tentando, primeiramente, praticar minhas habilidades de análise e, consequentemente, obter insights sobre a realidade das empresas do Brasil.

## Objetivos:
### Gerais:
Esse repositório foi criado com o objetivo de ser um back-end que disponibiliza um endpoint REST contendo:
* As informações demográficas do Brasil (Estados, Mesorregiões, Microrregiões e Cidades);
* As informações de CNAEs (Seção, Divisão, Grupo, Classe e Subclasse);
* As informações das empresas do Estado do Paraná.

### Específicos:
* Entender o contexto atual das empresas do Estado do Paraná;
* Obter insights sobre mercados em expansão, mercados em retração, concentração de empresas de certos segmentos, entre outros;

## Limitações:
Como o volume de dados é absolutamente massivo, optei por trabalhar apenas com as empresas do Estado do Paraná, que estão ativas ou que fecharam a partir de 01/01/2016.

### Uso:
O repositório contém apenas o código fonte para processamento e exibição dos dados. Os dados em si precisam ser baixados de suas respectivas fontes:
#### Dados geográficos:
Os dados geográficos podem ser obtidos no repositório [Monicipios-Brasileiros](https://github.com/pedrounes1/Municipios-Brasileiros) e os arquivos `csv` devem ser copiados para `./app/seeders/cidades`

#### Dados CNAEs:
Os dados de Cnaes e suas subdivisões podem ser obtidos no repositório [CNAES-IBGE-2_3](https://github.com/pedrounes1/CNAES-IBGE-2_3). Os arquivos `.csv` também devem ser salvos em `./app/seeders/cnaes`

#### Dados CNPJs:
Os dados de CNPJ podem ser obtidos no repositorio do [George Santiago](https://github.com/georgevbsantiago/qsacnpj). Nesse repo, você deve baixar a *Base de dados do CNPJ - CSV*, extrair os arquivos e salvar o arquivo `cnpj_dados_cadastrais_pj.csv` em `./app/seeders/cnpjs` como `cnpjs.csv`


#### Instalação:
Depois de baixar todos os arquivos, renomeie o arquivo `docker-config.yml` para `docker-compose.yml` e rode o comando `docker-compose up` no terminal de dentro da pasta com o código fonte.

Após a criação dos containers, crie um banco no postgres chamado `cnpj` e execute o arquivo `app/seeder.py` para inserir todos os dados no banco. Esse processo só precisa ser realizado uma vez.

O container, por padrão, responde a requisições REST na porta 65432.

## TODO:
[ ] Finalizar os endpoints para CNAEs e CNPJs;
[ ] Documentar a API
[ ] Finalizar a análise dos dados
[ ] Criar uma rotina de testes