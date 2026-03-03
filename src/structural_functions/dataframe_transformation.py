from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional, Union
import pandas as pd

Number = Union[int, float]


def _to_date(d: Union[str, date, datetime]) -> date:
    """Aceita date/datetime/ISO string (YYYY-MM-DD)."""
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        # tenta ISO primeiro (YYYY-MM-DD)
        try:
            return datetime.fromisoformat(d[:10]).date()
        except ValueError as e:
            raise ValueError(f"Data inválida: {d!r}. Use 'YYYY-MM-DD' ou date/datetime.") from e
    raise TypeError(f"Tipo inválido para data: {type(d).__name__}")


def valoracao_calc(
    *,
    data: Union[str, date, datetime],
    midia: str,
    alcance_organico: Optional[Number],
    tipo_midia_post: Optional[str] = None,     # ex: "photo", "album", "link", "video", "status", "text"
    duracao_video: Optional[Number] = None,    # segundos
    valoracao_radio_tv: Optional[Number] = None,
) -> float:
    """
    Implementa a lógica da "versão 2025.01.06" exatamente como no script.
    Retorna 0.0 quando não houver regra aplicável.

    Regras por data:
      - data < 2020-01-01: regras antigas (Facebook por faixas, Instagram por faixas, etc.)
      - data < 2024-12-01: regras intermediárias (Facebook por tipo, Instagram power, etc.)
      - data > 2024-11-30: regras mais recentes (inclui album/text e TV)
    """
    d = _to_date(data)
    midia = (midia or "").strip()
    tipo = (tipo_midia_post or "").strip().lower()

    # se alcance for None/NaN, trata como 0 (mantém retorno 0 no final)
    if alcance_organico is None:
        alcance = None
    else:
        try:
            alcance = float(alcance_organico)
        except (TypeError, ValueError):
            alcance = None

    def per_mil(mult: float) -> float:
        if alcance is None:
            return 0.0
        return (alcance / 1000.0) * float(mult)

    # ----------------- BLOCO 1: antes de 2020-01-01 -----------------
    if d < date(2020, 1, 1):
        if midia == "Facebook":
            if alcance is None:
                return 0.0
            if alcance < 2700:
                return (alcance / 1000.0) * 2.70
            elif 2700 <= alcance < 7200:
                return (alcance / 1000.0) * 2.93
            elif 7200 <= alcance < 19000:
                return (alcance / 1000.0) * 3.21
            elif 19000 <= alcance < 51000:
                return (alcance / 1000.0) * 4.06
            elif 51000 <= alcance < 130000:
                return (alcance / 1000.0) * 5.52
            elif 130000 <= alcance < 340000:
                return (alcance / 1000.0) * 7.66
            elif alcance >= 340000:
                return (alcance / 1000.0) * 9.92

        elif midia == "Instagram":
            if alcance is None:
                return 0.0
            if alcance < 2100:
                return (alcance / 1000.0) * 0.74
            elif 2100 <= alcance < 5500:
                return (alcance / 1000.0) * 0.79
            elif 5500 <= alcance < 14000:
                return (alcance / 1000.0) * 0.72
            elif 14000 <= alcance < 37000:
                return (alcance / 1000.0) * 0.90
            elif 37000 <= alcance < 99000:
                return (alcance / 1000.0) * 0.93
            elif 99000 <= alcance < 263000:
                return (alcance / 1000.0) * 1.14
            elif alcance >= 263000:
                return (alcance / 1000.0) * 5.05

        elif midia == "YouTube":
            return per_mil(56)

        elif midia == "Twitter":
            return per_mil(19.6)

        elif midia == "Online":
            return per_mil(33.27)

        elif midia == "Impresso":
            return per_mil(106.25)

        return 0.0

    # ----------------- BLOCO 2: antes de 2024-12-01 -----------------
    if d < date(2024, 12, 1):
        if midia == "Facebook" and tipo == "photo":
            return per_mil(13.38)

        elif midia == "Facebook" and tipo == "link":
            return per_mil(13.40)

        elif midia == "Facebook" and tipo == "video":
            return per_mil(11.22)

        elif midia == "Facebook" and tipo == "status":
            return per_mil(12.87)

        elif midia == "Instagram":
            # ([Alcance]/1000)*(322*(Alcance^-0.268))
            if alcance is None or alcance <= 0:
                return 0.0
            return (alcance / 1000.0) * (322.0 * (alcance ** (-0.268)))

        elif midia == "YouTube":
            if alcance is None:
                return 0.0
            dur = float(duracao_video or 0)
            if dur <= 269:
                return per_mil(75)
            elif dur <= 299:
                return per_mil(80)
            elif dur <= 359:
                return per_mil(85)
            elif dur <= 419:
                return per_mil(90)
            elif dur <= 509:
                return per_mil(95)
            else:
                return per_mil(100)

        elif midia == "Twitter":
            return per_mil(24.61)

        elif midia == "Online":
            if alcance is None:
                return 0.0
            return min((alcance / 1000.0) * 190.03, 146818.0)

        elif midia == "Impresso":
            if alcance is None or alcance <= 0:
                return 0.0
            val = 530225.0 * (alcance ** (-0.584))
            if val > 3229.12:
                return per_mil(3229.12)
            elif val < 90.34:
                return per_mil(90.34)
            else:
                return (alcance / 1000.0) * val

        return 0.0

    # ----------------- BLOCO 3: depois de 2024-11-30 -----------------
    # (equivale a d >= 2024-12-01)
    if midia == "Facebook" and (tipo == "photo" or tipo == "album"):
        return per_mil(13.38)

    elif midia == "Facebook" and tipo == "link":
        return per_mil(13.40)

    elif midia == "Facebook" and tipo == "video":
        return per_mil(11.22)

    elif midia == "Facebook" and (tipo == "status" or tipo == "text"):
        return per_mil(12.87)

    elif midia == "Instagram":
        if alcance is None or alcance <= 0:
            return 0.0
        return (alcance / 1000.0) * (322.0 * (alcance ** (-0.268)))

    elif midia == "YouTube":
        if alcance is None:
            return 0.0
        dur = float(duracao_video or 0)
        if dur <= 269:
            return per_mil(75)
        elif dur <= 299:
            return per_mil(80)
        elif dur <= 359:
            return per_mil(85)
        elif dur <= 419:
            return per_mil(90)
        elif dur <= 509:
            return per_mil(95)
        else:
            return per_mil(100)

    elif midia == "Twitter":
        return per_mil(24.61)

    elif midia == "Online":
        if alcance is None:
            return 0.0
        return min((alcance / 1000.0) * 190.03, 146818.0)

    elif midia == "Impresso":
        if alcance is None or alcance <= 0:
            return 0.0
        val = 530225.0 * (alcance ** (-0.584))
        if val > 3229.12:
            return per_mil(3229.12)
        elif val < 90.34:
            return per_mil(90.34)
        else:
            return (alcance / 1000.0) * val

    elif midia == "TV":
        try:
            return float(valoracao_radio_tv or 0.0)
        except (TypeError, ValueError):
            return 0.0

    return 0.0



def transform_dataframe(path_onboarding_dataframe: str) -> pd.DataFrame: 

    dataframe = pd.read_parquet(f'df_classificado.parquet')
    dataframe = dataframe.rename(columns={'Protagonismo' : 'Nível de Protagonismo final', 
                                        'tags_positividade' : 'Sentimento',
                                        'tags_promocao' : 'Tipos de impactos',
                                        'Visualizações' : 'Alcance orgânico',
                                        'marcas' : 'Empresa analisada'})

    dataframe['Mídia'] = 'Online'
    dataframe['Veículo'] = dataframe['Veículo (Default)']  
    dataframe['Produto analisado'] = 'None'
    dataframe['Tier'] = 'Tier 1'
    dataframe['Jornalista'] = 'Outros'
    dataframe['Ação'] =  'Outros'
    dataframe['Tipo da ação'] =  'Outros'
    dataframe['Status classificação'] = 'Classificado'
    dataframe['Valoração'] = dataframe.apply(
        lambda r: valoracao_calc(
            data=r['Data'],
            midia=r['Mídia'],
            alcance_organico=r['Alcance orgânico']    ),
        axis=1
    )

    prot_map = {'Coadjuvante' : 'Citação relevante', 
                'Muito Protagonista' : 'Protagonismo', 
                'Protagonista' : 'Protagonismo', 
                'Figurante' : 'Figurante'}

    dataframe['Nível de Protagonismo final'] = dataframe['Nível de Protagonismo final'].apply(lambda x : prot_map[x])

    map_impacto = {'Promotor' : 'Promotores', 
                'Inócuo' : 'Inócuos',
                'Detrator' : 'Detratores'

    }

    dataframe['Tipos de impactos'] = dataframe['Tipos de impactos'].apply(lambda x : map_impacto[x])
    path_name = path_onboarding_dataframe.split('.parquet')[0]
    
    dataframe.to_parquet(path_name + '_transformed' + '.parquet', index=False)
    return dataframe