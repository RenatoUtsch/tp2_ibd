#!/usr/bin/env python3
"""Carrega os dados para o banco."""

import collections
import csv
import datetime
import logging
import sys

import sqlalchemy as sa

import dados_rj

# Código do município do Rio de Janeiro
CODIGO_MUNICIPIO_RJ = 60011

ENGINE = sa.create_engine('mysql://ibd:ibd@localhost/tp2')
CONNECTION = ENGINE.connect()

METADATA = sa.MetaData(ENGINE)

ELEICAO = sa.Table('ELEICAO', METADATA,
                   sa.Column('id', sa.Integer, primary_key=True),
                   sa.Column('ano_eleicao', sa.Integer),
                   sa.Column('codigo_municipio', sa.Integer,
                             sa.ForeignKey('MUNICIPIO.codigo_municipio')),
                   sa.Column('codigo_cargo', sa.Integer,
                             sa.ForeignKey('CARGO.codigo_cargo')),
                   sa.Column('num_turno', sa.Integer),
                   sa.Column('descricao_eleicao', sa.String(32)))

CANDIDATO = sa.Table('CANDIDATO', METADATA,
                     sa.Column('id', sa.Integer, primary_key=True),
                     sa.Column('eleicao_id', sa.Integer,
                               sa.ForeignKey('ELEICAO.id')),
                     sa.Column('numero_candidato', sa.Integer),
                     sa.Column('nome_candidato', sa.String(64)),
                     sa.Column('nome_urna_candidato', sa.String(64)),
                     sa.Column('numero_partido', sa.Integer,
                               sa.ForeignKey('PARTIDO.numero_partido')),
                     sa.Column('numero_coligacao', sa.BigInteger,
                               sa.ForeignKey('COLIGACAO.numero_coligacao')))

VOTACAO = sa.Table('VOTACAO', METADATA,
                   sa.Column('id', sa.Integer, primary_key=True),
                   sa.Column('data_geracao', sa.DateTime),
                   sa.Column('numero_zona', sa.Integer),
                   sa.Column('candidato_id', sa.Integer,
                             sa.ForeignKey('CANDIDATO.id')),
                   sa.Column('total_votos', sa.Integer))

CARGO = sa.Table('CARGO', METADATA,
                 sa.Column('codigo_cargo', sa.Integer, primary_key=True),
                 sa.Column('descricao_cargo', sa.String(64)))

MUNICIPIO = sa.Table('MUNICIPIO', METADATA,
                     sa.Column(
                         'codigo_municipio', sa.Integer, primary_key=True),
                     sa.Column('nome_municipio', sa.String(64)),
                     sa.Column('sigla_uf', sa.String(2)))

PARTIDO = sa.Table('PARTIDO', METADATA,
                   sa.Column('numero_partido', sa.Integer, primary_key=True),
                   sa.Column('sigla_partido', sa.String(64)),
                   sa.Column('nome_partido', sa.String(64)))

COLIGACAO = sa.Table('COLIGACAO', METADATA,
                     sa.Column(
                         'numero_coligacao', sa.BigInteger, primary_key=True),
                     sa.Column('nome_coligacao', sa.String(128)))

COLIGACAO_COMPOSICAO = sa.Table(
    'COLIGACAO_COMPOSICAO', METADATA,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('numero_coligacao', sa.BigInteger,
              sa.ForeignKey('COLIGACAO.numero_coligacao')),
    sa.Column('numero_partido', sa.Integer,
              sa.ForeignKey('PARTIDO.numero_partido')))

RENDA = sa.Table('RENDA', METADATA,
                 sa.Column('id', sa.Integer, primary_key=True),
                 sa.Column('codigo_municipio', sa.Integer,
                           sa.ForeignKey('MUNICIPIO.codigo_municipio')),
                 sa.Column('valor_renda', sa.Float),
                 sa.Column('bairro', sa.String(50)),
                 sa.Column('numero_zona', sa.Integer))


class DadosCandidato(
        collections.namedtuple('DadosCandidato', [
            'data_geracao', 'ano_eleicao', 'num_turno', 'descricao_eleicao',
            'sigla_uf', 'sigla_ue', 'codigo_municipio', 'nome_municipio',
            'numero_zona', 'codigo_cargo', 'numero_cand', 'sq_candidato',
            'nome_candidato', 'nome_urna_candidato', 'descricao_cargo',
            'cod_sit_cand_superior', 'desc_sit_cand_superior',
            'codigo_sit_candidato', 'desc_sit_candidato',
            'codigo_sit_cand_tot', 'desc_sit_cand_tot', 'numero_partido',
            'sigla_partido', 'nome_partido', 'sequencial_legenda',
            'nome_coligacao', 'composicao_legenda', 'total_votos', 'transito'
        ])):
    """Entrada na database de VOTACAO_CANDIDATO_MUN_ZONA_2016_<UF>.

    Fields:
        data_geracao: datetime.datetime("02/06/2017 19:36:40")
        ano_eleicao: 2016
        num_turno: 1
        descricao_eleicao: "ELEIÇÕES MUNICIPAIS 2016"
        sigla_uf: "AC"
        sigla_ue: "01007"
        codigo_municipio: 01007
        nome_municipio: "BUJARI"
        numero_zona: 9
        codigo_cargo: 11
        numero_cand: 65
        sq_candidato: 10000001880
        nome_candidato: "ROMUALDO DE SOUZA ARAUJO"
        nome_urna_candidato: "ROMUALDO"
        descricao_cargo: "PREFEITO"
        cod_sit_cand_superior: 12
        desc_sit_cand_superior: "APTO"
        codigo_sit_candidato: 2
        desc_sit_candidato: "DEFERIDO"
        codigo_sit_cand_tot: 1
        desc_sit_cand_tot: "ELEITO"
        numero_partido: 65
        sigla_partido: "PC do B"
        nome_partido: "Partido Comunista do Brasil"
        sequencial_legenda: 10000000176
        nome_coligacao: "UNIDOS POR BUJARI"
        composicao_legenda: ["PC do B", "PRB", "PR", "PHS", "PEN", "PTC"]
        total_votos: 1853
        transito: "N"
    """


class Eleicao(
        collections.namedtuple('Eleicao', [
            'ano_eleicao', 'codigo_municipio', 'codigo_cargo', 'num_turno',
            'descricao_eleicao'
        ])):
    """Representa uma eleição."""


class Candidato(
        collections.namedtuple('Candidato', [
            'id', 'eleicao_id', 'numero_candidato', 'nome_candidato',
            'nome_urna_candidato', 'numero_partido', 'numero_coligacao'
        ])):
    """Representa um candidato."""


class Votacao(
        collections.namedtuple('Votacao', [
            'id', 'data_geracao', 'numero_zona', 'candidato_id', 'total_votos'
        ])):
    """Representa uma votação."""


class Cargo(
        collections.namedtuple('Cargo', ['codigo_cargo', 'descricao_cargo'])):
    """Representa um cargo."""


class Municipio(
        collections.namedtuple(
            'Municipio', ['codigo_municipio', 'nome_municipio', 'sigla_uf'])):
    """Representa um município."""


class Partido(
        collections.namedtuple(
            'Partido', ['numero_partido', 'sigla_partido', 'nome_partido'])):
    """Representa um partido."""


class Coligacao(
        collections.namedtuple('Coligacao', [
            'numero_coligacao', 'nome_coligacao', 'composicao_coligacao'
        ])):
    """Representa uma coligacao."""


class ZonaEleitoral(
        collections.namedtuple('ZonaEleitoral',
                               ['codigo_municipio', 'numero_zona', 'bairro'])):
    """Representa uma zona eleitoral."""


class Renda(
        collections.namedtuple('Renda', [
            'id', 'codigo_municipio', 'valor_renda', 'bairro', 'numero_zona'
        ])):
    """Representa uma renda."""


def carrega_base_candidato(filename):
    """Lê a base de candidatos com o formato DadosCandidato."""
    with open(filename, 'r', newline='', encoding='ISO-8859-1') as input_file:
        base_candidato = []
        for row in csv.reader(input_file, delimiter=';'):
            base_candidato.append(
                DadosCandidato(
                    data_geracao=datetime.datetime.strptime(
                        '{} {}'.format(row[0], row[1]), '%d/%m/%Y %H:%M:%S'),
                    ano_eleicao=int(row[2]),
                    num_turno=int(row[3]),
                    descricao_eleicao=row[4],
                    sigla_uf=row[5],
                    sigla_ue=row[6],
                    codigo_municipio=int(row[7]),
                    nome_municipio=row[8],
                    numero_zona=int(row[9]),
                    codigo_cargo=int(row[10]),
                    numero_cand=int(row[11]),
                    sq_candidato=int(row[12]),
                    nome_candidato=row[13],
                    nome_urna_candidato=row[14],
                    descricao_cargo=row[15],
                    cod_sit_cand_superior=int(row[16]),
                    desc_sit_cand_superior=row[17],
                    codigo_sit_candidato=int(row[18]),
                    desc_sit_candidato=row[19],
                    codigo_sit_cand_tot=int(row[20]),
                    desc_sit_cand_tot=row[21],
                    numero_partido=int(row[22]),
                    sigla_partido=row[23],
                    nome_partido=row[24],
                    sequencial_legenda=int(row[25]),
                    nome_coligacao=row[26],
                    composicao_legenda=row[27].split(' / '),
                    total_votos=int(row[28]),
                    transito=row[29]))
    return base_candidato


def recupera_eleicao(dados, eleicoes_por_id, ids_por_eleicao):
    """Preenche dicts de eleições, retorna id da eleição."""
    eleicao = Eleicao(dados.ano_eleicao, dados.codigo_municipio,
                      dados.codigo_cargo, dados.num_turno,
                      dados.descricao_eleicao)
    if eleicao not in ids_por_eleicao:
        eleicao_id = len(ids_por_eleicao) + 1
        ids_por_eleicao[eleicao] = eleicao_id
        eleicoes_por_id[eleicao_id] = eleicao
    else:
        eleicao_id = ids_por_eleicao[eleicao]

    return eleicao_id


def recupera_candidato(dados, candidato_por_chave, eleicao_id):
    """Preenche dict de (eleicao_id, numero_candidato) para candidato."""
    chave = eleicao_id, dados.numero_cand
    if chave not in candidato_por_chave:
        candidato_id = len(candidato_por_chave) + 1
        candidato_por_chave[chave] = Candidato(
            candidato_id, eleicao_id, dados.numero_cand, dados.nome_candidato,
            dados.nome_urna_candidato, dados.numero_partido,
            dados.sequencial_legenda)


def recupera_votacao(dados, votacoes, eleicao_id, candidato_por_chave):
    """Preenche lista de votações."""
    candidato = candidato_por_chave[eleicao_id, dados.numero_cand]
    votacao_id = len(votacoes) + 1
    votacoes.append(
        Votacao(votacao_id, dados.data_geracao, dados.numero_zona,
                candidato.id, dados.total_votos))


def recupera_cargo(dados, cargos):
    """Preenche set de cargos."""
    cargo = Cargo(dados.codigo_cargo, dados.descricao_cargo)
    if cargo not in cargos:
        cargos.add(cargo)


def recupera_municipio(dados, municipios):
    """Preenche set de municípios."""
    municipio = Municipio(dados.codigo_municipio, dados.nome_municipio,
                          dados.sigla_uf)
    if municipio not in municipios:
        municipios.add(municipio)


def recupera_partido(dados, partido_por_sigla):
    """Preenche dict de sigla de partido para Partido."""
    if dados.sigla_partido not in partido_por_sigla:
        partido_por_sigla[dados.sigla_partido] = Partido(
            dados.numero_partido, dados.sigla_partido, dados.nome_partido)


def recupera_coligacao(dados, coligacao_por_numero):
    """Preenche dict de numero_coligacao para coligação."""
    if dados.sequencial_legenda not in coligacao_por_numero:
        coligacao_por_numero[dados.sequencial_legenda] = Coligacao(
            numero_coligacao=dados.sequencial_legenda,
            nome_coligacao=dados.nome_coligacao,
            composicao_coligacao=dados.composicao_legenda)


def recupera_zonas_eleitorais():
    """Retorna lista de zonas."""
    linhas = [i.strip() for i in dados_rj.ze.split(';')][:-1]

    zonas = []
    for linha in linhas:
        numero_zona, nome_bairro = linha.split('-')
        zonas.append(
            ZonaEleitoral(CODIGO_MUNICIPIO_RJ, numero_zona, nome_bairro))

    return zonas


def recupera_rendas(zonas):
    """Retorna lista de rendas."""
    linhas = [i.strip() for i in dados_rj.renda.split(';')][:-1]

    renda_por_chave = {}
    for linha in linhas:
        bairros, renda = linha.split('-')
        for nome in bairros.split(','):
            renda_por_chave[CODIGO_MUNICIPIO_RJ, nome] = renda

    renda_id = 1
    rendas = []
    for zona in zonas:
        try:
            valor_renda = renda_por_chave[zona.codigo_municipio, zona.bairro]
        except KeyError:
            logging.exception('Bairro com renda não encontrado.')
            continue

        rendas.append(
            Renda(renda_id, zona.codigo_municipio, valor_renda, zona.bairro,
                  zona.numero_zona))
        renda_id += 1

    return rendas


def insere_eleicoes(eleicoes_por_id):
    """Insere eleições na database."""
    for eleicao_id, eleicao in eleicoes_por_id.items():
        CONNECTION.execute(ELEICAO.insert().values(
            id=eleicao_id,
            ano_eleicao=eleicao.ano_eleicao,
            codigo_municipio=eleicao.codigo_municipio,
            codigo_cargo=eleicao.codigo_cargo,
            num_turno=eleicao.num_turno,
            descricao_eleicao=eleicao.descricao_eleicao))


def insere_candidatos(candidato_por_chave):
    """Insere candidatos na database."""
    for candidato in candidato_por_chave.values():
        CONNECTION.execute(CANDIDATO.insert().values(
            id=candidato.id,
            eleicao_id=candidato.eleicao_id,
            numero_candidato=candidato.numero_candidato,
            nome_candidato=candidato.nome_candidato,
            nome_urna_candidato=candidato.nome_urna_candidato,
            numero_partido=candidato.numero_partido,
            numero_coligacao=candidato.numero_coligacao))


def insere_votacoes(votacoes):
    """Insere votações na database."""
    for votacao in votacoes:
        CONNECTION.execute(VOTACAO.insert().values(
            id=votacao.id,
            data_geracao=votacao.data_geracao,
            numero_zona=votacao.numero_zona,
            candidato_id=votacao.candidato_id,
            total_votos=votacao.total_votos))


def insere_cargos(cargos):
    """Insere cargos na database."""
    for cargo in cargos:
        CONNECTION.execute(CARGO.insert().values(
            codigo_cargo=cargo.codigo_cargo,
            descricao_cargo=cargo.descricao_cargo))


def insere_municipios(municipios):
    """Insere municípios na database."""
    for municipio in municipios:
        CONNECTION.execute(MUNICIPIO.insert().values(
            codigo_municipio=municipio.codigo_municipio,
            nome_municipio=municipio.nome_municipio,
            sigla_uf=municipio.sigla_uf))


def insere_partidos(partido_por_sigla):
    """Insere partidos na database."""
    for partido in partido_por_sigla.values():
        CONNECTION.execute(PARTIDO.insert().values(
            numero_partido=partido.numero_partido,
            sigla_partido=partido.sigla_partido,
            nome_partido=partido.nome_partido))


def insere_coligacoes(coligacao_por_numero, partido_por_sigla):
    """Insere coligações."""
    composicao_id = 1
    for coligacao in coligacao_por_numero.values():
        CONNECTION.execute(COLIGACAO.insert().values(
            numero_coligacao=coligacao.numero_coligacao,
            nome_coligacao=coligacao.nome_coligacao))

        for sigla in coligacao.composicao_coligacao:
            partido = partido_por_sigla[sigla]
            CONNECTION.execute(COLIGACAO_COMPOSICAO.insert().values(
                id=composicao_id,
                numero_coligacao=coligacao.numero_coligacao,
                numero_partido=partido.numero_partido))
            composicao_id += 1


def insere_rendas(rendas):
    """Insere rendas."""
    for renda in rendas:
        CONNECTION.execute(RENDA.insert().values(
            id=renda.id,
            codigo_municipio=renda.codigo_municipio,
            valor_renda=renda.valor_renda,
            bairro=renda.bairro,
            numero_zona=renda.numero_zona))


def main():
    """Ponto de entrada do programa."""
    base_candidato = carrega_base_candidato(sys.argv[1])

    eleicoes_por_id = {}
    ids_por_eleicao = {}
    candidato_por_chave = {}
    votacoes = []
    cargos = set()
    municipios = set()
    partido_por_sigla = {}
    coligacao_por_numero = {}
    for dados in base_candidato:
        eleicao_id = recupera_eleicao(dados, eleicoes_por_id, ids_por_eleicao)
        recupera_candidato(dados, candidato_por_chave, eleicao_id)
        recupera_votacao(dados, votacoes, eleicao_id, candidato_por_chave)
        recupera_cargo(dados, cargos)
        recupera_municipio(dados, municipios)
        recupera_partido(dados, partido_por_sigla)
        recupera_coligacao(dados, coligacao_por_numero)

    zonas = recupera_zonas_eleitorais()
    rendas = recupera_rendas(zonas)

    print('Término da recuperação de dados. Pressione enter para inserir.')
    input()

    METADATA.create_all()

    insere_partidos(partido_por_sigla)
    insere_coligacoes(coligacao_por_numero, partido_por_sigla)
    insere_municipios(municipios)
    insere_cargos(cargos)
    insere_eleicoes(eleicoes_por_id)
    insere_candidatos(candidato_por_chave)
    insere_votacoes(votacoes)
    insere_rendas(rendas)


if __name__ == '__main__':
    main()
