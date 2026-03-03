import pandas as pd

def nps_score(
    df: pd.DataFrame,
    value_col: str = "alcance",
    impacto_col: str = "Tipos de impactos",
    group_cols=("Data", "Empresa analisada")
) -> pd.DataFrame:
    """
    Calcula o score de alcance:
    (Promotores - Detratores) / (Promotores + Detratores + Inócuos)

    - Usa 'alcance' como valor
    - Trata impactos ausentes como 0
    """

    # Pivotar impactos para colunas
    pivot = (
        df.pivot_table(
            index=list(group_cols),
            columns=impacto_col,
            values=value_col,
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    # Garantir colunas obrigatórias
    for col in ["Promotores", "Detratores", "Inócuos"]:
        if col not in pivot.columns:
            pivot[col] = 0

    # Cálculo do score
    denom = pivot["Promotores"] + pivot["Detratores"] + pivot["Inócuos"]

    pivot["nps_score"] = (
        (pivot["Promotores"] - pivot["Detratores"]) / denom.replace(0, pd.NA)
    ).fillna(0).round(2)

    return pivot


import pandas as pd
from typing import Iterable, Optional, Union, List

import pandas as pd
from typing import Iterable

def nps_total_and_contrib(
    df: pd.DataFrame,
    dim_col: str,  # ex: "Temas"
    value_col: str = "alcance",
    impacto_col: str = "Tipos de impactos",
    group_cols: Iterable[str] = ("Data", "Empresa analisada"),
    impacts=("Promotores", "Detratores", "Inócuos"),
    contr_type = 'Total', # "Total" ou "Promotor"
    round_total: int = 2,
    round_contrib: int = 4,
) -> pd.DataFrame:
    """
    Output:
      group_cols + [dim_col] + ["nps_score", f"nps_contrib_{dim_col}", "denom_total"]

    Onde:
      nps_score = (Promotores - Detratores) / (Promotores + Detratores + Inócuos)  (total do período+empresa)
      nps_contrib_dim = (Promotores_dim - Detratores_dim) / denom_total
    """

    df = df.copy()
    dim_contrib_col = f"nps_contrib_{dim_col}"

    # -------------------------
    # 1) NPS TOTAL (por group_cols)
    # -------------------------
    total = (
        df.pivot_table(
            index=list(group_cols),
            columns=impacto_col,
            values=value_col,
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    for c in impacts:
        if c not in total.columns:
            total[c] = 0

    total["denom_total"] = total[list(impacts)].sum(axis=1)

    total["nps_score"] = (
        (total["Promotores"] - total["Detratores"])
        / total["denom_total"].replace(0, pd.NA)
    ).fillna(0).round(round_total)

    total_keep = total[list(group_cols) + ["denom_total", "nps_score"]]

    # -------------------------
    # 2) CONTRIBUIÇÃO (por group_cols + dim_col)
    # -------------------------
    
    if contr_type == "Total":
        dim_pivot = (
            df.pivot_table(
                index=list(group_cols) + [dim_col],
                columns=impacto_col,
                values=value_col,
                aggfunc="sum",
                fill_value=0
            )
            .reset_index()
        )

        for c in impacts:
            if c not in dim_pivot.columns:
                dim_pivot[c] = 0

        out = dim_pivot.merge(total_keep, on=list(group_cols), how="left")

        out[dim_contrib_col] = (
            (out["Promotores"] - out["Detratores"])
            / out["denom_total"].replace(0, pd.NA)
        ).fillna(0).round(round_contrib)

        # mantém só o que você pediu
        out = out[list(group_cols) + [dim_col, "nps_score", dim_contrib_col, "denom_total"]]

        return out.sort_values(list(group_cols) + [dim_col]).reset_index(drop=True)


    if contr_type == "Promotor":
        dim_pivot = (
            df.pivot_table(
                index=list(group_cols) + [dim_col],
                columns=impacto_col,
                values=value_col,
                aggfunc="sum",
                fill_value=0
            )
            .reset_index()
        )

        for c in impacts:
            if c not in dim_pivot.columns:
                dim_pivot[c] = 0

        out = dim_pivot.merge(total_keep, on=list(group_cols), how="left")

        out[dim_contrib_col] = (
            (out["Promotores"] )
            / out["denom_total"].replace(0, pd.NA)
        ).fillna(0).round(round_contrib)

        # mantém só o que você pediu
        out = out[list(group_cols) + [dim_col, "nps_score", dim_contrib_col, "denom_total"]]

        return out.sort_values(list(group_cols) + [dim_col]).reset_index(drop=True)


def protagonism_score(
    df: pd.DataFrame,
    value_col: str = "alcance",
    impacto_col: str = "Nível de Protagonismo final",
    group_cols=("Data", "Empresa analisada"),
    filter_column = None,
    filter_value = None
) -> pd.DataFrame:
    """
    Calcula a valoração pelo período

    - Usa 'valoração' como valor
    - Trata impactos ausentes como 0
    """
    if filter_column is not None and filter_value is not None:
        if isinstance(filter_value, (list, tuple, set)):
            df = df[df[filter_column].isin(filter_value)]
        else:
            df = df[df[filter_column].eq(filter_value)]


    # Pivotar impactos para colunas
    pivot = (
        df.pivot_table(
            index=list(group_cols),
            columns=impacto_col,
            values=value_col,
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    # Garantir colunas obrigatórias
    for col in ['Citação relevante', 'Figurante', 'Referência contextual / Setor',
       'Protagonismo', 'Referência em matéria de concorrente']:
        if col not in pivot.columns:
            pivot[col] = 0

    # Cálculo do score

    denom = pivot['Citação relevante'] + pivot['Figurante'] + pivot['Referência contextual / Setor'] + pivot['Protagonismo'] + pivot['Referência em matéria de concorrente']

    denom = denom.astype(float)
    pivot["protagonism_score"] = (((pivot['Protagonismo'] + pivot['Referência contextual / Setor']) / denom.where(denom != 0))
                                .fillna(0).round(4) )

    pivot['total'] = denom

    return pivot



def freq_score(
    df: pd.DataFrame,
    value_col: str = "count",
    impacto_col: str = "Tipos de impactos",
    group_cols=("Data", "Empresa analisada")
) -> pd.DataFrame:
    """
    Calculo da Frequência:
  
    - Usa 'Count' como valor
    - Trata ausentes como 0
    """

    # Pivotar impactos para colunas
    pivot = (
        df.pivot_table(
            index=list(group_cols),
            columns=impacto_col,
            values=value_col,
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )
    if 'Promotores' in pivot.columns:
        pivot['total'] =  pivot['Detratores'] + pivot['Inócuos'] + pivot['Promotores']
        pivot['% Promotores'] = pivot['Promotores'] / pivot['total']
        pivot['% Detratores'] = pivot['Detratores'] / pivot['total']
        pivot['% Inócuos'] = pivot['Inócuos'] / pivot['total']
    
    
    if 'Positivo' in pivot.columns:
        pivot['total'] =  pivot['Positivo'] + pivot['Negativo'] + pivot['Neutro'] + pivot['-']
        pivot['% Positivo'] =  pivot['Positivo'] / pivot['total']
        pivot['% Negativo'] =  pivot['Negativo'] / pivot['total']
        pivot['% Neutro'] =  pivot['Neutro'] / pivot['total']
    
    return pivot

def valoration_score(
    df: pd.DataFrame,
    value_col: str = "valoracao",
    group_cols=("Data", "Empresa analisada")
) -> pd.DataFrame:
    """
    Calcula o score de alcance:
    (Promotores - Detratores) / (Promotores + Detratores + Inócuos)

    - Usa 'alcance' como valor
    - Trata impactos ausentes como 0
    """

    # Pivotar impactos para colunas
    pivot = (
        df.pivot_table(
            index=list(group_cols),
            values=value_col,
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )
    pivot['valoracao'] = pivot['valoracao'].apply(lambda x: round(x, 2))
    return pivot


def jornalista_score(
    df: pd.DataFrame,
    value_cols=('jornalista_count', 'count'),
    group_cols=("Data", "Empresa analisada")
) -> pd.DataFrame:
    """
    Soma métricas de jornalista

    - Usa múltiplas colunas de valor (ex: jornalista_count, count)
    - Agrupa por Data / Empresa analisada
    - Trata valores ausentes como 0
    """

    pivot = (
        df.pivot_table(
            index=list(group_cols),
            values=list(value_cols),
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    return pivot


def action_score(
    df: pd.DataFrame,
    value_cols=('acao_count', 'count'),
    group_cols=("Data", "Empresa analisada")
) -> pd.DataFrame:
    """
    Count de ação
    - Usa 'action_count' como valor
    - Trata impactos ausentes como 0
    """

    # Pivotar impactos para colunas
    pivot = (
        df.pivot_table(
            index=list(group_cols),
            values=list(value_cols),
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )
    return pivot



def is_jornalist(jornalista):
    list_not_jornalista = ['não mapeado', '-', 'redação', 'indeterminado']
    jornalista_lower = jornalista.lower()

    if any(l.lower() in jornalista_lower for l in list_not_jornalista):
        return None
    else: 
        return 'é jornalista'
    
def is_action_count(action):
    list_not_action = ['outros', '-', 'indeterminado']
    action_lower = action.lower()

    if any(l.lower() in action_lower for l in list_not_action):
        return None
    else: 
        return 'é ação'
    


def is_action(action):
    list_not_action = ['outros', '-']
    action_lower = action.lower()

    if any(l.lower() in action_lower for l in list_not_action):
        return 'sem ação'
    else: 
        return 'ação'  