from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from utils.funcoes import criaEngine
from sqlalchemy.sql.schema import MetaData


engine = criaEngine()
meta = MetaData(bind=engine)
Base = declarative_base(metadata=meta)


class Estado(Base):
    __tablename__ = 'estados'
    _prefixo = 'estado_'

    id = Column(Integer, primary_key=True)
    estado = Column(String(50))
    latitude = Column(String(15))
    longitude = Column(String(15))
    uf = Column(String(2), unique=True)
    regiao = Column(String(20), nullable=True)

    mesos = relationship('Mesorregiao', back_populates='estado', cascade="all, delete-orphan", passive_deletes=True)
    micros = relationship('Microrregiao', back_populates='estado', cascade="all, delete-orphan", passive_deletes=True)
    cidades = relationship('Cidade', back_populates='estado', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return f"<Estado(id={self.id}, nome={self.nome}, uf={self.uf}, regiao={self.regiao})>"


class Mesorregiao(Base):
    __tablename__ = 'mesorregioes'
    _prefixo = "meso_"

    id = Column(Integer, primary_key=True)
    ibge_id = Column(Integer)
    nome = Column(String(100))
    estado_id = Column(Integer, ForeignKey('estados.id', onupdate="CASCADE", ondelete="CASCADE"))

    estado = relationship('Estado', back_populates="mesos", cascade="all", passive_deletes=True)

    micros = relationship('Microrregiao', back_populates="meso", cascade="all", passive_deletes=True)
    cidades = relationship('Cidade', back_populates="meso", cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return f"<Mesorregiao(id={self.id}, ibge_id={self.ibge_id}, nome={self.nome},estado_id={self.estado_id})>"


class Microrregiao(Base):
    __tablename__ = 'microrregioes'
    _prefixo = "micro_"

    id = Column(Integer, primary_key=True)
    ibge_id = Column(Integer)
    nome = Column(String(100))
    estado_id = Column(Integer, ForeignKey(
        'estados.id', onupdate="CASCADE", ondelete="CASCADE"))
    meso_id = Column(Integer, ForeignKey('mesorregioes.id', onupdate="CASCADE", ondelete="CASCADE"))

    estado = relationship('Estado', back_populates="micros", cascade="all", passive_deletes=True)
    meso = relationship('Mesorregiao', back_populates="micros", cascade="all", passive_deletes=True)
    cidades = relationship('Cidade', back_populates="micro", cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return f"<Microrregiao(id={self.id}, ibge_id={self.ibge_id}, nome={self.nome},estado_id={self.estado_id}, meso_id={self.meso_id})>"


class Cidade(Base):
    __tablename__ = 'cidades'
    _prefixo = 'cidade_'

    id = Column(Integer, primary_key=True)
    siafi_id = Column(Integer, unique=True)
    nome = Column(String(100))
    estado_id = Column(Integer, ForeignKey('estados.id', onupdate="CASCADE", ondelete="CASCADE"))
    meso_id = Column(Integer, ForeignKey('mesorregioes.id', onupdate="CASCADE", ondelete="CASCADE"))
    micro_id = Column(Integer, ForeignKey('microrregioes.id', onupdate="CASCADE", ondelete="CASCADE"))
    ddd = Column(String(2))
    fuso_horario = Column(String(32))
    capital = Column(Boolean)
    latitude = Column(String(15))
    longitude = Column(String(15))

    estado = relationship('Estado', back_populates="cidades", cascade="all", passive_deletes=True)
    meso = relationship('Mesorregiao', back_populates="cidades", cascade="all", passive_deletes=True)
    micro = relationship('Microrregiao', back_populates="cidades", cascade="all", passive_deletes=True)
    cnpjs = relationship('Cnpj', back_populates='siafi', cascade="all", passive_deletes=True)

    def __repr__(self):
        return f"<Cidade(id={self.id}, cnpj_id={self.siafi_id}, nome={self.nome},estado_id={self.estado_id}, meso_id={self.meso_id}, micro_id={self.micro_id})>"


class CnaeSecao(Base):
    __tablename__ = 'cnae_secoes'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    _prefixo = 'cnae_secao_'

    id = Column(String(1), primary_key=True)
    descricao = Column(String(255))

    divisoes = relationship('CnaeDivisao', back_populates='secao', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<CnaeSecao(id={}, cod_secao={}, descricao={})>".format(self.id, self.cod_secao, self.descricao)


class CnaeDivisao(Base):
    __tablename__ = 'cnae_divisoes'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    _prefixo = 'cnae_divisao_'

    id = Column(Integer, primary_key=True)
    descricao = Column(String(255))
    secao_id = Column(String(1), ForeignKey('cnae_secoes.id', onupdate="CASCADE", ondelete="CASCADE"))

    secao = relationship('CnaeSecao', back_populates='divisoes', cascade="all", passive_deletes=True)
    grupos = relationship('CnaeGrupo', back_populates='divisao', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<CnaeDivisao(id={}, descricao={}, secao_id={})>".format(self.id, self.descricao, self.secao_id)


class CnaeGrupo(Base):
    __tablename__ = 'cnae_grupos'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    _prefixo = 'cnae_grupo_'

    id = Column(Integer, primary_key=True)
    cod_grupo = Column(String(5))
    descricao = Column(String(255))
    divisao_id = Column(Integer, ForeignKey('cnae_divisoes.id', onupdate="CASCADE", ondelete="CASCADE"))

    divisao = relationship('CnaeDivisao', back_populates='grupos', cascade="all", passive_deletes=True)
    classes = relationship('CnaeClasse', back_populates='grupo', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<CnaeGrupo(id={}, cod_grupo={}, descricao={}, divisao_id{})>".format(self.id, self.cod_grupo, self.descricao, self.divisao_id)


class CnaeClasse(Base):
    __tablename__ = 'cnae_classes'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    _prefixo = 'cnae_classe_'

    id = Column(Integer, primary_key=True)
    cod_classe = Column(String(10))
    descricao = Column(String(255))
    grupo_id = Column(Integer, ForeignKey('cnae_grupos.id',
                                          onupdate="CASCADE", ondelete="CASCADE"))

    grupo = relationship('CnaeGrupo', back_populates='classes', cascade="all", passive_deletes=True)
    cnaes = relationship('Cnae', back_populates='classe', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<CnaeClasse(id={}, cod_classe={}, descricao={}, grupo_id={})>".format(self.id, self.cod_classe, self.descricao, self.grupo_id)


class Cnae(Base):
    __tablename__ = 'cnaes'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    _prefixo = 'cnae_subclasse_'

    id = Column(Integer, primary_key=True)
    descricao = Column(String(255))
    classe_id = Column(Integer, ForeignKey('cnae_classes.id', onupdate="CASCADE", ondelete="CASCADE"))

    classe = relationship('CnaeClasse', back_populates='cnaes', cascade="all", passive_deletes=True)
    cnpjs = relationship('Cnpj', back_populates='cnae', cascade='all, delete-orphan')

    def __repr__(self):
        return "<Cnae(id={},  descricao={}, classe_id={})>".format(self.id, self.descricao, self.classe_id)


class MotivoSituacaoCadastral(Base):
    __tablename__ = 'situacoes_cadastrais_motivos'
    __table_args__ = {'mysql_engine': 'InnoDB'}

    id = Column(Integer, primary_key=True)
    descricao = Column(String(100))

    cnpjs = relationship('Cnpj', back_populates='situacao_motivo', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<MotivoSituacaoCadastral(id={}, codigo={}, descricao={})>".format(self.id, self.codigo, self.descricao)


class SituacaoCadastral(Base):
    __tablename__ = 'situacoes_cadastrais'
    __table_args__ = {'mysql_engine': 'InnoDB'}

    id = Column(Integer, primary_key=True)
    descricao = Column(String(100))

    cnpjs = relationship('Cnpj', back_populates='situacao_cadastral', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<SituacaoCadastral(id={}, descricao={})>".format(self.id, self.descricao)


class NaturezaJuridica(Base):
    __tablename__ = 'naturezas_juridicas'
    __table_args__ = {'mysql_engine': 'InnoDB'}

    id = Column(Integer, primary_key=True)
    descricao = Column(String(100))

    cnpjs = relationship('Cnpj', back_populates='natureza_juridica', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<NaturezaJuridica(id={}, descricao={})>".format(self.id, self.descricao)


class Qualificacao(Base):
    __tablename__ = 'qualificacoes'
    __table_args__ = {'mysql_engine': 'InnoDB'}

    id = Column(Integer, primary_key=True)
    cod_qualificacao = Column(Integer, unique=True)
    descricao = Column(String(255))
    coletado = Column(Boolean)

    cnpjs = relationship('Cnpj', back_populates='qualificacao', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<Qualificacao(id={},  descricao={}, coletado={})>".format(self.id, self.descricao, self.coletado)


class Porte(Base):
    __tablename__ = 'portes'
    __table_args__ = {'mysql_engine': 'InnoDB'}

    id = Column(Integer, primary_key=True)
    codigo = Column(Integer, unique=True)
    descricao = Column(String(255))

    cnpjs = relationship('Cnpj', back_populates='porte', cascade="all, delete-orphan", passive_deletes=True)

    def __repr__(self):
        return "<Porte(id={},  descricao={})>".format(self.id, self.descricao)


# Tabela principal de CNPJ:
class Cnpj(Base):

    __tablename__ = 'pessoas_juridicas'
    __table_args__ = {'mysql_engine': 'InnoDB'}

    id = Column(Integer(), primary_key=True)
    cnpj = Column(String(16), unique=True)
    matriz = Column(Boolean)
    razao_social = Column(String(150))
    nome_fantasia = Column(String(55))
    data_situacao_cadastral = Column(Date)
    situacao_id = Column(Integer(), ForeignKey('situacoes_cadastrais.id', onupdate="CASCADE", ondelete="CASCADE"))
    situacao_motivo_id = Column(Integer(), ForeignKey(MotivoSituacaoCadastral.id, onupdate="CASCADE", ondelete="CASCADE"))
    natureza_id = Column(Integer(), ForeignKey(NaturezaJuridica.id, onupdate="CASCADE", ondelete="CASCADE"))
    data_inicio_atividade = Column(Date)
    cnae_id = Column(Integer(), ForeignKey(Cnae.id, onupdate="CASCADE", ondelete="CASCADE"))
    # tipo_logradouro = Column(String(20))
    # logradouro = Column(String(60))
    # numero = Column(String(6), nullable=True)
    # complemento = Column(String(156), nullable=True)
    # bairro = Column(String(50), nullable=True)
    cep = Column(String(8))
    siafi_id = Column(Integer(), ForeignKey(Cidade.siafi_id, onupdate="CASCADE", ondelete="CASCADE"))
    # ddd_telefone_1 = Column(String(12), nullable=True)
    # ddd_telefone_2 = Column(String(12), nullable=True)
    # ddd_fax = Column(String(12), nullable=True)
    # email = Column(String(115), nullable=True)
    qualificacao_id = Column(Integer(), ForeignKey(Qualificacao.cod_qualificacao, onupdate="CASCADE", ondelete="CASCADE"))
    capital_social = Column(Float, nullable=True)
    porte_id = Column(Integer(), ForeignKey('portes.codigo', onupdate="CASCADE", ondelete="CASCADE"))
    # simples = Column(Integer(), nullable=True)
    # data_opcao_simples = Column(Date, nullable=True)
    # data_exclusao_simples = Column(Date, nullable=True)
    mei = Column(Boolean)
    situacao_especial = Column(String(23), nullable=True)
    data_situacao_especial = Column(Date, nullable=True)

    situacao_cadastral = relationship('SituacaoCadastral', back_populates='cnpjs', cascade="all", passive_deletes=True)
    situacao_motivo = relationship('MotivoSituacaoCadastral', back_populates='cnpjs', cascade="all", passive_deletes=True)
    natureza_juridica = relationship('NaturezaJuridica', back_populates='cnpjs', cascade="all", passive_deletes=True)
    siafi = relationship('Cidade', back_populates='cnpjs', cascade="all", passive_deletes=True)
    cnae = relationship('Cnae', back_populates='cnpjs', cascade='all', passive_deletes=True)
    qualificacao = relationship('Qualificacao', back_populates='cnpjs', cascade="all", passive_deletes=True)
    porte = relationship('Porte', back_populates='cnpjs', cascade="all", passive_deletes=True)


def inicia():
    Base.metadata.create_all(criaEngine())


def deleta():
    Base.metadata.drop_all(criaEngine())
