from src.structural_functions.index_function import nps_score, nps_total_and_contrib, protagonism_score, freq_score, valoration_score
from src.structural_functions.index_function import jornalista_score, action_score
from src.structural_functions.index_function import is_jornalist, is_action, is_action_count
import pandas as pd
import hashlib


### funções auxiliares

def generate_row_hash(
    df: pd.DataFrame,
    columns: list,
    hash_col: str = "hash_id",
    sep: str = "||"
) -> pd.DataFrame:
    """
    Gera um hash SHA-256 baseado em múltiplas colunas do DataFrame.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame de entrada
    columns : list
        Lista de colunas usadas para gerar o hash
    hash_col : str
        Nome da coluna de hash gerada
    sep : str
        Separador usado na concatenação

    Retorna
    -------
    pd.DataFrame
        DataFrame com a coluna de hash adicionada
    """

    def _hash_row(row):
        normalized = [
            str(row[col]).strip().lower() if pd.notna(row[col]) else ""
            for col in columns
        ]
        joined = sep.join(normalized)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    df[hash_col] = df.apply(_hash_row, axis=1)
    return df


map_mes = {
    1 : 'Jan',
    2 : 'Feb',
    3 : 'Mar',
    4 : 'Apr',
    5 : 'May',
    6 : 'Jun',
    7 : 'Jul',
    8 : 'Aug',
    9 : 'Sep',
    10 : 'Oct',
    11 : 'Nov', 
    12 : 'Dec'
}

map_mes_days = {
    1 : 31,
    2 : 28,
    3 : 31,
    4 : 30,
    5 : 31,
    6 : 30,
    7 : 31,
    8 : 31,
    9 : 30,
    10 : 31,
    11 : 30, 
    12 : 31
}


############ funções principais

### gerando as dataviews

def gen_dataviews(path_dataframe: str) -> pd.DataFrame:
    '''
    input: path_dataframe .parquet
    output: Dataview (dataframe)
    
    '''
    dataframe = pd.read_parquet(path_dataframe)
    
    if 'Chave Análise de Mídia Hash' not in  dataframe.columns:
    
        cols_hash = [
            "Data",
            "Link",
            "Mídia",
            "Veículo",
            "Veículo (Default)"
        ]

        dataframe = generate_row_hash(dataframe, columns=cols_hash, hash_col='Chave Análise de Mídia Hash')

    if dataframe['Alcance orgânico'].dtype != 'float64':
        dataframe['Alcance orgânico'] = dataframe['Alcance orgânico'].apply(lambda x : float(x.replace('-', '0').replace(',', '.')))
    
    if dataframe['Valoração'].dtype != 'float64':        
        dataframe['Valoração'] = dataframe['Valoração'].apply(lambda x : float(x.replace('-', '0').replace(',', '.')))

    if any(col in dataframe.columns for col in ['Jornalista', 'Ação']):
    
        dataframe['Jornalista count'] = dataframe['Jornalista'].apply(lambda x : is_jornalist(x))
        dataframe['Ação total'] = dataframe['Ação'].apply(lambda x : is_action_count(x))
        dataframe['É ação'] = dataframe['Ação'].apply(lambda x : is_action(x))

    columns_fix = ['Data', 'Tipos de impactos', 'Sentimento', 'Empresa analisada', 'Produto analisado',
                                'Nível de Protagonismo final', 'Tier', 'Jornalista', 'Mídia', 'Ação', 'Tipo da ação', 'É ação', 'Status classificação']

    #columns_values = ['Chave Análise de Mídia Hash', 'Alcance orgânico', 'Valoração', 'Jornalista count', 'Ação total']

    columns_themes = [
        "Macro assunto", "Macro-assunto", "Temas", "Mensagem-chave", "Tags",
        "Pilar", "Sub-pilar", "Subpilar (Crise)", "Subpilar (Geral)",
        "Tópicos", "Pilares", "Assuntos monitorados"
    ]

    columns_themes_valid = [c for c in columns_themes if c in dataframe.columns]
    columns_valid = columns_fix + columns_themes_valid

    dataview = dataframe.groupby(columns_valid).agg(
        alcance=('Alcance orgânico', 'sum'),
        valoracao=('Valoração', 'sum'),
        count=('Chave Análise de Mídia Hash', 'count'),
        jornalista_count=('Jornalista count', 'count'),
        acao_count=('Ação total', 'count')
        ).reset_index()

    # garante que é datetime
    dataview["Data"] = pd.to_datetime(dataview["Data"])

    # cria ano e mês
    dataview["ano"] = dataview["Data"].dt.year
    dataview["mes"] = dataview["Data"].dt.month

    return dataview


### calcular as metas

from typing import Dict

def goals_calculations(dataview: pd.DataFrame) -> Dict:
    
    
    ### DATAVIEWS

    ### NPS, Promotores, Detratores
    ### calculando os dataviews do NPS: agg_mes, agg_ano, agg_dia 

    agg_mes_nps_score = nps_score(dataview, group_cols=('mes', 'ano', 'Empresa analisada'))
    agg_mes_nps_score['mes_name'] = agg_mes_nps_score['mes'].apply(lambda x : map_mes[x])
    agg_mes_nps_score['data'] = agg_mes_nps_score.apply(lambda x : x['mes_name'] + '/' + str((x['ano'])), axis=1)
    period = agg_mes_nps_score['data'].astype(str).str.strip().str.lower().pipe(lambda s: pd.to_datetime(s, format="%b/%Y", errors="coerce")).dt.to_period("M")
    agg_mes_nps_score["data"] = period
    agg_ano_nps_score = nps_score(dataview, group_cols=('ano', 'Empresa analisada'))
    agg_mes_nps_score['% Promotores'] = agg_mes_nps_score['Promotores'] / (agg_mes_nps_score['Promotores'] + agg_mes_nps_score['Detratores'] + agg_mes_nps_score['Inócuos'])
    agg_mes_nps_score['% Detratores'] = agg_mes_nps_score['Detratores'] / (agg_mes_nps_score['Promotores'] + agg_mes_nps_score['Detratores'] + agg_mes_nps_score['Inócuos'])
    agg_ano_nps_score['% Promotores'] = agg_ano_nps_score['Promotores'] / (agg_ano_nps_score['Promotores'] + agg_ano_nps_score['Detratores'] + agg_ano_nps_score['Inócuos'])
    agg_ano_nps_score['% Detratores'] = agg_ano_nps_score['Detratores'] / (agg_ano_nps_score['Promotores'] + agg_ano_nps_score['Detratores'] + agg_ano_nps_score['Inócuos'])


    ### Protagonismo (total e promotor)
    ### calculando os dataviews do Protagonismo: agg_mes, agg_ano, agg_dia 

    #protagonismo total
    agg_mes_protagonismo_score = protagonism_score(dataview,  group_cols=("mes", "ano", "Empresa analisada")) #, filter_column='Tipos de impactos', filter_value='Promotores'
    agg_mes_protagonismo_score = agg_mes_protagonismo_score.sort_values(by=['ano'], ascending=True)
    agg_mes_protagonismo_score['mes_name'] = agg_mes_protagonismo_score['mes'].apply(lambda x : map_mes[x])
    agg_mes_protagonismo_score['data'] = agg_mes_protagonismo_score.apply(lambda x : x['mes_name'] + '/' + str((x['ano'])), axis=1)
    period = agg_mes_protagonismo_score['data'].astype(str).str.strip().str.lower().pipe(lambda s: pd.to_datetime(s, format="%b/%Y", errors="coerce")).dt.to_period("M")
    agg_mes_protagonismo_score["data"] = period
    agg_ano_protagonismo_score = protagonism_score(dataview, group_cols=('ano', 'Empresa analisada'))

    # protagonismo promotor
    agg_mes_protagonismo_promotor_score = protagonism_score(dataview,  group_cols=("mes", "ano", "Empresa analisada"), filter_column='Tipos de impactos', filter_value='Promotores')
    agg_mes_protagonismo_promotor_score = agg_mes_protagonismo_promotor_score.sort_values(by=['ano'], ascending=True)
    agg_mes_protagonismo_promotor_score['mes_name'] = agg_mes_protagonismo_promotor_score['mes'].apply(lambda x : map_mes[x])
    agg_mes_protagonismo_promotor_score['data'] = agg_mes_protagonismo_promotor_score.apply(lambda x : x['mes_name'] + '/' + str((x['ano'])), axis=1)
    period = agg_mes_protagonismo_promotor_score['data'].astype(str).str.strip().str.lower().pipe(lambda s: pd.to_datetime(s, format="%b/%Y", errors="coerce")).dt.to_period("M")
    agg_mes_protagonismo_promotor_score["data"] = period
    agg_ano_protagonismo_promotor_score = protagonism_score(dataview, group_cols=('ano', 'Empresa analisada'), filter_column='Tipos de impactos', filter_value='Promotores')


    ### Frequência 
    ##### calculando os dataviews do Frequência: agg_mes, agg_ano, agg_dia 
    agg_mes_freq_score = freq_score(dataview, impacto_col="Tipos de impactos", group_cols=("mes", "ano", "Empresa analisada"))
    agg_mes_freq_score = agg_mes_freq_score.sort_values(by=['ano'], ascending=True)
    agg_mes_freq_score['mes_name'] = agg_mes_freq_score['mes'].apply(lambda x : map_mes[x])
    agg_mes_freq_score['data'] = agg_mes_freq_score.apply(lambda x : x['mes_name'] + '/' + str((x['ano'])), axis=1)
    period = agg_mes_freq_score['data'].astype(str).str.strip().str.lower().pipe(lambda s: pd.to_datetime(s, format="%b/%Y", errors="coerce")).dt.to_period("M")
    agg_mes_freq_score['dias'] = agg_mes_freq_score['mes'].apply(lambda x : map_mes_days[x])
    agg_mes_freq_score["data"] = period
    agg_mes_freq_score['Freq. Media por Dia'] = agg_mes_freq_score['total'] / agg_mes_freq_score['dias']
    agg_mes_freq_score['Freq. Promotora Media por Dia'] = agg_mes_freq_score['Promotores'] / agg_mes_freq_score['dias']
    agg_mes_freq_score['Freq. Detratora Media por Dia'] = agg_mes_freq_score['Detratores'] / agg_mes_freq_score['dias']


    ### Valoracao 
    ##### calculando os dataviews do Valoracao: agg_mes, agg_ano, agg_dia 

    agg_mes_valoration_score = valoration_score(dataview,  group_cols=("mes", "ano", "Empresa analisada"))
    agg_mes_valoration_score = agg_mes_valoration_score.sort_values(by=['ano'], ascending=True)
    agg_mes_valoration_score['mes_name'] = agg_mes_valoration_score['mes'].apply(lambda x : map_mes[x])
    agg_mes_valoration_score['data'] = agg_mes_valoration_score.apply(lambda x : x['mes_name'] + '/' + str((x['ano'])), axis=1)
    period = agg_mes_valoration_score['data'].astype(str).str.strip().str.lower().pipe(lambda s: pd.to_datetime(s, format="%b/%Y", errors="coerce")).dt.to_period("M")
    agg_mes_valoration_score["data"] = period
    agg_ano_valoration_score = valoration_score(dataview,  group_cols=("ano", "Empresa analisada"))
    agg_dia_valoration_score = valoration_score(dataview,  group_cols=("Data", "Empresa analisada"))

    ### Contribuição de Ações  
    ##### calculando os dataviews: agg_mes, agg_ano, agg_dia 

    agg_mes_nps_acao_total = nps_total_and_contrib(
        dataview,
        dim_col='É ação',
        group_cols=("ano", "mes", "Empresa analisada"),
        contr_type='Total'
    )

    try:
        agg_mes_nps_acao_per_total = agg_mes_nps_acao_total[agg_mes_nps_acao_total['É ação'].isin(['ação'])].reset_index()
        agg_mes_nps_acao_per_total['per de contr das ações'] = round(agg_mes_nps_acao_per_total['nps_contrib_É ação'] / agg_mes_nps_acao_per_total['nps_score'] *100, 2)
        agg_mes_nps_acao_per_total = agg_mes_nps_acao_per_total.sort_values(by=['ano'], ascending=True)
        agg_mes_nps_acao_per_total['mes_name'] = agg_mes_nps_acao_per_total['mes'].apply(lambda x : map_mes[x])
        agg_mes_nps_acao_per_total['data'] = agg_mes_nps_acao_per_total.apply(lambda x : x['mes_name'] + '/' + str((x['ano'])), axis=1)
        period = agg_mes_nps_acao_per_total['data'].astype(str).str.strip().str.lower().pipe(lambda s: pd.to_datetime(s, format="%b/%Y", errors="coerce")).dt.to_period("M")
        agg_mes_nps_acao_per_total["data"] = period


        agg_ano_nps_acao_total = nps_total_and_contrib(
            dataview,
            dim_col='É ação',
            group_cols=("ano", "Empresa analisada"),
            contr_type='Total'
        )

        agg_ano_nps_acao_per_total = agg_ano_nps_acao_total[agg_ano_nps_acao_total['É ação'].isin(['ação'])].reset_index()
        agg_ano_nps_acao_per_total['per de contr das ações'] = round(agg_ano_nps_acao_per_total['nps_contrib_É ação'] / agg_ano_nps_acao_per_total['nps_score'] *100, 2)


        ### Contribuição de Ações Promotoras
        ##### calculando os dataviews: agg_mes, agg_ano, agg_dia 


        agg_mes_nps_acao_promotor = nps_total_and_contrib(
            dataview,
            dim_col='É ação',
            group_cols=("ano", "mes", "Empresa analisada"),
            contr_type='Promotor'
        )

        agg_mes_nps_acao_per_promotor = agg_mes_nps_acao_promotor[agg_mes_nps_acao_promotor['É ação'].isin(['ação'])].reset_index()
        agg_mes_nps_acao_per_promotor['per de contr das ações'] = round(agg_mes_nps_acao_per_promotor['nps_contrib_É ação'] / agg_mes_nps_acao_per_promotor['nps_score'] *100, 2)
        agg_mes_nps_acao_per_promotor = agg_mes_nps_acao_per_promotor.sort_values(by=['ano'], ascending=True)
        agg_mes_nps_acao_per_promotor['mes_name'] = agg_mes_nps_acao_per_promotor['mes'].apply(lambda x : map_mes[x])
        agg_mes_nps_acao_per_promotor['data'] = agg_mes_nps_acao_per_promotor.apply(lambda x : x['mes_name'] + '/' + str((x['ano'])), axis=1)
        period = agg_mes_nps_acao_per_promotor['data'].astype(str).str.strip().str.lower().pipe(lambda s: pd.to_datetime(s, format="%b/%Y", errors="coerce")).dt.to_period("M")
        agg_mes_nps_acao_per_promotor["data"] = period

        agg_mes_nps_acao_per_promotor

        agg_ano_nps_acao_promotor = nps_total_and_contrib(
            dataview,
            dim_col='É ação',
            group_cols=("ano", "Empresa analisada"),
            contr_type='Promotor'
        )

        agg_ano_nps_acao_per_promotor = agg_ano_nps_acao_promotor[agg_ano_nps_acao_promotor['É ação'].isin(['ação'])].reset_index()
        agg_ano_nps_acao_per_promotor['per de contr das ações'] = round(agg_ano_nps_acao_per_promotor['nps_contrib_É ação'] / agg_ano_nps_acao_per_promotor['nps_score'] *100, 2)
    
    ### Contr. de Ações para o NPS
        ### gerando valores contr_total_acao_media_mensal, contr_total_acao_acumulada_ano
        ### contr_promotora_acao_media_mensal, contr_promotora_acao_acumulada_ano

        # %_de_contr_total_acao_media_mensal
        descr_contr_total_acao_media_mensal = 'É o indicador que mensura o quanto as ações de comunicação influenciam a construção da reputação da marca ao longo do período. A definição de uma média mensal como meta garante a manutenção das ações em um patamar adequado, evitando variações significativas ao longo dos meses.'
        contr_total_acao_media_mensal_baseline = round(agg_mes_nps_acao_per_total['per de contr das ações'].mean(),2) 
        contr_total_acao_media_mensal_conservador_meta = round(contr_total_acao_media_mensal_baseline - (agg_mes_nps_acao_per_total['per de contr das ações'].std() / 5), 2)
        contr_total_acao_media_mensal_conservador_variacao = round((contr_total_acao_media_mensal_conservador_meta - contr_total_acao_media_mensal_baseline) / contr_total_acao_media_mensal_baseline *100, 2) 
        contr_total_acao_media_mensal_moderado_meta = round(contr_total_acao_media_mensal_baseline*1.02, 2)
        contr_total_acao_media_mensal_moderado_variacao = round((contr_total_acao_media_mensal_moderado_meta - contr_total_acao_media_mensal_baseline) / contr_total_acao_media_mensal_baseline *100, 2) 
        contr_total_acao_media_mensal_ousado_meta = round(contr_total_acao_media_mensal_baseline + (agg_mes_nps_acao_per_total['per de contr das ações'].std() / 5), 2)
        contr_total_acao_media_mensal_ousado_variacao = round((contr_total_acao_media_mensal_ousado_meta - contr_total_acao_media_mensal_baseline) / contr_total_acao_media_mensal_baseline *100, 2) 


        # %_de_agg_ano_nps_acao_per_total
        descr_contr_total_acao_acumulada_ano = 'É o indicador que mensura o quanto as ações de comunicação influenciam a construção da reputação da marca ao longo do ano. A meta acumulada anual permite avaliar esse impacto de forma consolidada, reduzindo a influência de variações pontuais entre os meses.'
        contr_total_acao_agg_ano_baseline = round(agg_ano_nps_acao_per_total['per de contr das ações'].mean(),2) 
        contr_total_acao_agg_ano_conservador_meta = round(contr_total_acao_agg_ano_baseline*0.98, 2)
        contr_total_acao_agg_ano_conservador_variacao = round((contr_total_acao_agg_ano_conservador_meta - contr_total_acao_agg_ano_baseline) / contr_total_acao_agg_ano_baseline *100, 2) 
        contr_total_acao_agg_ano_moderado_meta = round(contr_total_acao_agg_ano_baseline*1.02, 2)
        contr_total_acao_agg_ano_moderado_variacao = round((contr_total_acao_agg_ano_moderado_meta - contr_total_acao_agg_ano_baseline) / contr_total_acao_agg_ano_baseline *100, 2) 
        contr_total_acao_agg_ano_ousado_meta = round(contr_total_acao_agg_ano_baseline*1.05, 2)
        contr_total_acao_agg_ousado_variacao = round((contr_total_acao_agg_ano_ousado_meta - contr_total_acao_agg_ano_baseline) / contr_total_acao_agg_ano_baseline *100, 2) 


        # %_de_contr_promotora_acao_media_mensal
        descr_contr_promotora_acao_media_mensal = 'É o indicador que mensura o quanto as ações de comunicação que geraram impacto promotor influenciam a construção da reputação da marca ao longo do período. A definição de uma média mensal como meta garante a manutenção das ações em um patamar adequado, evitando variações significativas ao longo dos meses.'
        contr_promotor_acao_media_mensal_baseline = round(agg_mes_nps_acao_per_promotor['per de contr das ações'].mean(),2) 
        contr_promotor_acao_media_mensal_conservador_meta = round(contr_promotor_acao_media_mensal_baseline - (agg_mes_nps_acao_per_promotor['per de contr das ações'].std() / 5), 2)
        contr_promotor_acao_media_mensal_conservador_variacao = round((contr_promotor_acao_media_mensal_conservador_meta - contr_promotor_acao_media_mensal_baseline) / contr_promotor_acao_media_mensal_baseline *100, 2) 
        contr_promotor_acao_media_mensal_moderado_meta = round(contr_promotor_acao_media_mensal_baseline*1.02, 2)
        contr_promotor_acao_media_mensal_moderado_variacao = round((contr_promotor_acao_media_mensal_moderado_meta - contr_promotor_acao_media_mensal_baseline) / contr_promotor_acao_media_mensal_baseline *100, 2) 
        contr_promotor_acao_media_mensal_ousado_meta = round(contr_promotor_acao_media_mensal_baseline + (agg_mes_nps_acao_per_promotor['per de contr das ações'].std() / 5), 2)
        contr_promotor_acao_media_mensal_ousado_variacao = round((contr_promotor_acao_media_mensal_ousado_meta - contr_promotor_acao_media_mensal_baseline) / contr_promotor_acao_media_mensal_baseline *100, 2) 

        # %_de_agg_ano_nps_acao_per_promotor
        descr_contr_promotor_acao_acumulada_ano = 'É o indicador que mensura o quanto as ações de comunicação que geraram impacto promotor influenciam a construção da reputação da marca ao longo do ano. A meta acumulada anual permite avaliar esse impacto de forma consolidada, reduzindo a influência de variações pontuais entre os meses.'
        contr_promotor_acao_agg_ano_baseline = round(agg_ano_nps_acao_per_promotor['per de contr das ações'].mean(),2) 
        contr_promotor_acao_agg_ano_conservador_meta = round(contr_promotor_acao_agg_ano_baseline*0.98, 2)
        contr_promotor_acao_agg_ano_conservador_variacao = round((contr_promotor_acao_agg_ano_conservador_meta - contr_promotor_acao_agg_ano_baseline) / contr_promotor_acao_agg_ano_baseline *100, 2) 
        contr_promotor_acao_agg_ano_moderado_meta = round(contr_promotor_acao_agg_ano_baseline*1.02, 2)
        contr_promotor_acao_agg_ano_moderado_variacao = round((contr_promotor_acao_agg_ano_moderado_meta - contr_promotor_acao_agg_ano_baseline) / contr_promotor_acao_agg_ano_baseline *100, 2) 
        contr_promotor_acao_agg_ano_ousado_meta = round(contr_promotor_acao_agg_ano_baseline*1.05, 2)
        contr_promotor_acao_agg_ano_ousado_variacao = round((contr_promotor_acao_agg_ano_ousado_meta - contr_promotor_acao_agg_ano_baseline) / contr_promotor_acao_agg_ano_baseline *100, 2) 

        contr_dict = {'ações de comunicação' :
                    { '(%) Contr. das Ações ao NPS - Média mensal' :
                                    
                                        { 'descricao' : descr_contr_total_acao_media_mensal, 
                                            'baseline' : contr_total_acao_media_mensal_baseline, 
                                            
                                            'conservadora' : {'meta' : contr_total_acao_media_mensal_conservador_meta, 
                                                        'variacao' : contr_total_acao_media_mensal_conservador_variacao} , 
                                        
                                        'moderada' : {'meta' : contr_total_acao_media_mensal_moderado_meta, 
                                                        'variacao' : contr_total_acao_media_mensal_moderado_variacao}, 

                                        'ousada' : {'meta' : contr_total_acao_media_mensal_ousado_meta,  
                                                        'variacao' : contr_total_acao_media_mensal_ousado_variacao}}, 



                    '(%) Contr. das Ações ao NPS - Acumulado no ano' :
                                    
                                        { 'descricao' : descr_contr_total_acao_acumulada_ano, 
                                            'baseline' : contr_total_acao_agg_ano_baseline, 
                                            
                                            'conservadora' : {'meta' : contr_total_acao_agg_ano_conservador_meta, 
                                                        'variacao' : contr_total_acao_agg_ano_conservador_variacao} , 
                                        
                                        'moderada' : {'meta' : contr_total_acao_agg_ano_moderado_meta, 
                                                        'variacao' : contr_total_acao_agg_ano_moderado_variacao}, 

                                        'ousada' : {'meta' : contr_total_acao_agg_ano_ousado_meta,  
                                                        'variacao' : contr_total_acao_agg_ousado_variacao}}, 



                    '(%) Contr. das Ações Promotoras ao NPS - Média mensal' :
                                    
                                        { 'descricao' : descr_contr_promotora_acao_media_mensal, 
                                            'baseline' : contr_promotor_acao_media_mensal_baseline, 
                                            
                                            'conservadora' : {'meta' : contr_promotor_acao_media_mensal_conservador_meta, 
                                                        'variacao' : contr_promotor_acao_media_mensal_conservador_variacao} , 
                                        
                                        'moderada' : {'meta' : contr_promotor_acao_media_mensal_moderado_meta, 
                                                        'variacao' : contr_promotor_acao_media_mensal_moderado_variacao}, 

                                        'ousada' : {'meta' : contr_promotor_acao_media_mensal_ousado_meta,  
                                                        'variacao' : contr_promotor_acao_media_mensal_ousado_variacao}
                                                        },
                    
                    
                    '(%) Contr. das Ações Promotoras ao NPS - Acumulado no ano' :
                                    
                                        { 'descricao' : descr_contr_promotor_acao_acumulada_ano, 
                                            'baseline' : contr_promotor_acao_agg_ano_baseline, 
                                            
                                            'conservadora' : {'meta' : contr_promotor_acao_agg_ano_conservador_meta, 
                                                        'variacao' : contr_promotor_acao_agg_ano_conservador_variacao} , 
                                        
                                        'moderada' : {'meta' : contr_promotor_acao_agg_ano_moderado_meta, 
                                                        'variacao' : contr_promotor_acao_agg_ano_moderado_variacao}, 

                                        'ousada' : {'meta' : contr_promotor_acao_agg_ano_ousado_meta,  
                                                        'variacao' : contr_promotor_acao_agg_ano_ousado_variacao}
                                                        },
                                                        
                                                        }, 



        }

    except:

        contr_dict = {}

    ########### DICIONÁRIOS ###########

    ## NPS
    ## gerando os valores de media_mensal_nps e  agregado_nps_ano
    descr_media_mensal_nps = 'O indicador é a média mensal do NPS. É ideal para observar o desempenho mensal, evitando desvios importantes no NPS e riscos de percepção da imagem da marca no período.'
    media_mensal_nps_baseline = round(agg_mes_nps_score['nps_score'].mean()*100, 2)
    media_mensal_nps_conservadora_meta = min(round((agg_mes_nps_score['nps_score'].mean()*1.02 - (agg_mes_nps_score['nps_score'].std() / 4))*100, 2), 100.00)
    media_mensal_nps_conservadora_variacao = round(media_mensal_nps_conservadora_meta - media_mensal_nps_baseline, 2) #round((agg_mes_nps_score['nps_score'].std() / 3)*100, 2)*-1
    media_mensal_nps_moderada_meta = min(round((agg_mes_nps_score['nps_score'].mean()*1.02)*100, 2), 100.00)
    media_mensal_nps_moderada_variacao =  round(media_mensal_nps_moderada_meta - media_mensal_nps_baseline, 2) # round((agg_mes_nps_score['nps_score'].mean()*0.05)*100, 2)
    media_mensal_nps_ousada_meta = min(round((agg_mes_nps_score['nps_score'].mean()*1.02 + (agg_mes_nps_score['nps_score'].std() / 5))*100, 2), 100.00)
    media_mensal_nps_ousada_variacao = round(media_mensal_nps_ousada_meta - media_mensal_nps_baseline, 2) # round((agg_mes_nps_score['nps_score'].std() / 4)*100, 2)

    descr_agregegado_nps = 'O indicador é o agregado do NPS no decorrer do ano. É ideal para observar o total da percepção no acumulada do ano sobre a marca.'
    agregado_nps_baseline = round(agg_ano_nps_score['nps_score'].mean()*100, 2)
    agregado_nps_ano_conservadora_meta = min(round(agg_ano_nps_score['nps_score'].mean()*0.98, 2)*100, 100.00)
    agregado_nps_ano_conservadora_varicao = round(agregado_nps_ano_conservadora_meta - agregado_nps_baseline, 2)
    agregado_nps_ano_moderada_meta = min(round(agg_ano_nps_score['nps_score'].mean()*1.02, 2)*100, 100.00)
    agregado_nps_ano_moderada_varicao = round(agregado_nps_ano_moderada_meta - agregado_nps_baseline, 2)
    agregado_nps_ano_ousada_meta = min(round(agg_ano_nps_score['nps_score'].mean()*1.05, 2)*100, 100.00)
    agregado_nps_ano_ousada_varicao = round(agregado_nps_ano_ousada_meta - agregado_nps_baseline, 2)


    NPS_dict= {'NPS - Média mensal' :  
                                    { 'descricao' : descr_media_mensal_nps, 
                                        'baseline' : media_mensal_nps_baseline, 
                                        
                                        'conservadora' : {'meta' : media_mensal_nps_conservadora_meta, 
                                                    'variacao' : media_mensal_nps_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : media_mensal_nps_moderada_meta, 
                                                    'variacao' : media_mensal_nps_moderada_variacao}, 

                                    'ousada' : {'meta' : media_mensal_nps_ousada_meta,  
                                                    'variacao' : media_mensal_nps_ousada_variacao}}, 
                
                            'NPS - Acumulado no ano' : {
                                    'descricao' : descr_agregegado_nps, 
                                    'baseline' : agregado_nps_baseline, 
                                    'conservadora' : {'meta' : agregado_nps_ano_conservadora_meta, 
                                                    'variacao' : agregado_nps_ano_conservadora_varicao} , 
                                    
                                    'moderada' : {'meta' : agregado_nps_ano_moderada_meta , 
                                                    'variacao' : agregado_nps_ano_moderada_varicao} , 
                
                                    'ousada' : {'meta' : agregado_nps_ano_ousada_meta , 
                                                    'variacao' : agregado_nps_ano_ousada_varicao}, 
                    
                }}


    ### IMPACTO PROMOTOR 
    ### gerando valores media_mensal_impacto_promotor; agregado_impacto_promotor; media de % Promotor; agregado de % Promotor
    # media_mensal_impacto_promotor
    descr_media_mensal_impacto_promotor = 'O indicador é a média mensal do impacto promotor da marca. É ideal para observar o desempenho mensal da percepção promotora, com o objetivo de reduzir desvios importantes e manter a promoção da imagem de forma constante no decorrer no ano'
    media_mensal_impacto_promotor_baseline = round(agg_mes_nps_score['Promotores'].mean(), 2)
    media_mensal_impacto_promotor_conservadora_meta = round(media_mensal_impacto_promotor_baseline -  (agg_mes_nps_score['Promotores'].std() / 25), 2)
    media_mensal_impacto_promotor_conservadora_variacao = round((media_mensal_impacto_promotor_conservadora_meta - media_mensal_impacto_promotor_baseline) / media_mensal_impacto_promotor_baseline * 100,2 )
    media_mensal_impacto_promotor_moderada_meta = round(media_mensal_impacto_promotor_baseline*1.02, 2)
    media_mensal_impacto_promotor_moderada_variacao = round((media_mensal_impacto_promotor_moderada_meta - media_mensal_impacto_promotor_baseline) / media_mensal_impacto_promotor_baseline * 100,2 )
    media_mensal_impacto_promotor_ousada_meta = round(media_mensal_impacto_promotor_baseline +  (agg_mes_nps_score['Promotores'].std() / 25), 2)
    media_mensal_impacto_promotor_ousada_variacao = round((media_mensal_impacto_promotor_ousada_meta - media_mensal_impacto_promotor_baseline) / media_mensal_impacto_promotor_baseline * 100,2 )

    # agregado_impacto_promotor
    descr_agregado_impacto_promotor = 'O indicador é a soma do impacto promotor da marca. É ideal para observar o total da percepção promotora acumulada no ano sobre a marca.'
    agregado_impacto_promotor_baseline = round(agg_mes_nps_score['Promotores'].sum(), 2)
    agregado_impacto_promotor_conservadora_meta = round(agregado_impacto_promotor_baseline*0.98, 2)
    agregado_impacto_promotor_conservadora_variacao = round((agregado_impacto_promotor_conservadora_meta - agregado_impacto_promotor_baseline) / agregado_impacto_promotor_baseline * 100,2 )
    agregado_impacto_promotor_moderada_meta = round(agregado_impacto_promotor_baseline*1.02, 2)
    agregado_impacto_promotor_moderada_variacao = round((agregado_impacto_promotor_moderada_meta - agregado_impacto_promotor_baseline) / agregado_impacto_promotor_baseline * 100,2 )
    agregado_impacto_promotor_ousada_meta = round(agregado_impacto_promotor_baseline*1.05, 2)
    agregado_impacto_promotor_ousada_variacao = round((agregado_impacto_promotor_ousada_meta - agregado_impacto_promotor_baseline) / agregado_impacto_promotor_baseline * 100,2 )

    # media_mensal_%_impacto_promotor
    descr_media_mensal_perc_impacto_promotor = 'O indicador é a média mensal do percentual do impacto promotor da marca. É ideal para observar o desempenho mensal da percepção promotora, com o objetivo de reduzir desvios importantes e manter a promoção da imagem de forma constante no decorrer no ano'
    media_mensal_perc_impacto_promotor_baseline = round(agg_mes_nps_score['% Promotores'].mean(), 2)
    media_mensal_perc_impacto_promotor_conservadora_meta = min(round(media_mensal_perc_impacto_promotor_baseline -  (agg_mes_nps_score['% Promotores'].std() / 4), 2), 100.00)
    media_mensal_perc_impacto_promotor_conservadora_variacao = round((media_mensal_perc_impacto_promotor_conservadora_meta - media_mensal_perc_impacto_promotor_baseline) * 100,2 )
    media_mensal_perc_impacto_promotor_moderada_meta = min(round(media_mensal_perc_impacto_promotor_baseline*1.02, 2), 100.00)
    media_mensal_perc_impacto_promotor_moderada_variacao = round((media_mensal_perc_impacto_promotor_moderada_meta - media_mensal_perc_impacto_promotor_baseline)  * 100,2 )
    media_mensal_perc_impacto_promotor_ousada_meta = min(round(media_mensal_perc_impacto_promotor_baseline +  (agg_mes_nps_score['% Promotores'].std() / 3), 2), 2)
    media_mensal_perc_impacto_promotor_ousada_variacao = round((media_mensal_perc_impacto_promotor_ousada_meta - media_mensal_perc_impacto_promotor_baseline) * 100,2 )


    # agregado de % Promotor
    descr_agregado_perc_impacto_promotor = 'O indicador é o percentual do impacto promotor da marca no ano. É ideal para observar o total da percepção promotora acumulada do ano sobre a marca.'
    agregado_perc_impacto_promotor_baseline = round(agg_ano_nps_score['% Promotores'].mean(), 2)
    agregado_perc_impacto_promotor_conservadora_meta = round(agregado_perc_impacto_promotor_baseline*0.98, 2)
    agregado_perc_impacto_promotor_conservadora_variacao = round((agregado_perc_impacto_promotor_conservadora_meta - agregado_perc_impacto_promotor_baseline)  * 100,2 )
    agregado_perc_impacto_promotor_moderada_meta = round(agregado_perc_impacto_promotor_baseline*1.02, 2)
    agregado_perc_impacto_promotor_moderada_variacao = round((agregado_perc_impacto_promotor_moderada_meta - agregado_perc_impacto_promotor_baseline)  * 100,2 )
    agregado_perc_impacto_promotor_ousada_meta = round(agregado_perc_impacto_promotor_baseline*1.05, 2)
    agregado_perc_impacto_promotor_ousada_variacao = round((agregado_perc_impacto_promotor_ousada_meta - agregado_perc_impacto_promotor_baseline)  * 100,2 )


    Impacto_Promotor_dict = {'Impressão Promotora - Média mensal' :  
                                    { 'descricao' : descr_media_mensal_impacto_promotor, 
                                        'baseline' : media_mensal_impacto_promotor_baseline, 
                                        
                                        'conservadora' : {'meta' : media_mensal_impacto_promotor_conservadora_meta, 
                                                    'variacao' : media_mensal_impacto_promotor_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : media_mensal_impacto_promotor_moderada_meta, 
                                                    'variacao' : media_mensal_impacto_promotor_moderada_variacao}, 

                                    'ousada' : {'meta' : media_mensal_impacto_promotor_ousada_meta,  
                                                    'variacao' : media_mensal_impacto_promotor_ousada_variacao}}, 
                
                            'Impressão Promotora - Acumulado no ano' : {
                                    'descricao' : descr_agregado_impacto_promotor, 
                                    'baseline' : agregado_impacto_promotor_baseline, 
                                    'conservadora' : {'meta' : agregado_impacto_promotor_conservadora_meta, 
                                                    'variacao' : agregado_impacto_promotor_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : agregado_impacto_promotor_moderada_meta , 
                                                    'variacao' : agregado_impacto_promotor_moderada_variacao} , 
                
                                    'ousada' : {'meta' : agregado_impacto_promotor_ousada_meta , 
                                                    'variacao' : agregado_impacto_promotor_ousada_variacao}}, 
                    
                            'Impressão Promotora (%) - Média mensal' : {
                                    'descricao' : descr_media_mensal_perc_impacto_promotor, 
                                    'baseline' : media_mensal_perc_impacto_promotor_baseline, 
                                    'conservadora' : {'meta' : media_mensal_perc_impacto_promotor_conservadora_meta, 
                                                    'variacao' : media_mensal_perc_impacto_promotor_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : media_mensal_perc_impacto_promotor_moderada_meta , 
                                                    'variacao' : media_mensal_perc_impacto_promotor_moderada_variacao} , 
                
                                    'ousada' : {'meta' : media_mensal_perc_impacto_promotor_ousada_meta , 
                                                    'variacao' : media_mensal_perc_impacto_promotor_ousada_variacao}, 

                            'Impressão Promotora (%) - Acumulado no ano' : {
                                    'descricao' : descr_agregado_perc_impacto_promotor, 
                                    'baseline' : agregado_perc_impacto_promotor_baseline, 
                                    'conservadora' : {'meta' : agregado_perc_impacto_promotor_conservadora_meta, 
                                                    'variacao' : agregado_perc_impacto_promotor_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : agregado_perc_impacto_promotor_moderada_meta , 
                                                    'variacao' : agregado_perc_impacto_promotor_moderada_variacao} , 
                
                                    'ousada' : {'meta' : agregado_perc_impacto_promotor_ousada_meta , 
                                                    'variacao' : agregado_perc_impacto_promotor_ousada_variacao}}, 


                }}

    ### IMPACTO DETRATOR 
    ### gerando valores media_mensal_impacto_detrator; agregado_impacto_detrator; media de % Detrator; agregado de % Detrator
    # media_mensal_impacto_detrator
    descr_media_mensal_impacto_detrator = 'O indicador é a média mensal do impacto detrator da marca. É ideal para observar o desempenho mensal da percepção crítica, com o objetivo de tentar identificar desvios importantes e reduzir picos de imagem negativa no decorrer no ano'
    media_mensal_impacto_detrator_baseline = round(agg_mes_nps_score['Detratores'].mean(), 2)
    media_mensal_impacto_detrator_conservadora_meta = round(media_mensal_impacto_detrator_baseline +  (agg_mes_nps_score['Detratores'].std() / 25), 2)
    media_mensal_impacto_detrator_conservadora_variacao = round((media_mensal_impacto_detrator_conservadora_meta - media_mensal_impacto_detrator_baseline) / media_mensal_impacto_detrator_baseline * 100,2 )
    media_mensal_impacto_detrator_moderada_meta = round(media_mensal_impacto_detrator_baseline*1.02, 2)
    media_mensal_impacto_detrator_moderada_variacao = round((media_mensal_impacto_detrator_moderada_meta - media_mensal_impacto_detrator_baseline) / media_mensal_impacto_detrator_baseline * 100,2 )
    media_mensal_impacto_detrator_ousada_meta = round(media_mensal_impacto_detrator_baseline -  (agg_mes_nps_score['Detratores'].std() / 25), 2)
    media_mensal_impacto_detrator_ousada_variacao = round((media_mensal_impacto_detrator_ousada_meta - media_mensal_impacto_detrator_baseline) / media_mensal_impacto_detrator_baseline * 100,2 )

    # agregado_impacto_detrator
    descr_agregado_impacto_detrator = 'O indicador é a soma do impacto detrator da marca. É ideal para observar o total da percepção crítica acumulada no ano sobre a marca.'
    agregado_impacto_detrator_baseline = round(agg_mes_nps_score['Detratores'].sum(), 2)
    agregado_impacto_detrator_conservadora_meta = round(agregado_impacto_promotor_baseline*1.05, 2)
    agregado_impacto_detrator_conservadora_variacao = round((agregado_impacto_detrator_conservadora_meta - agregado_impacto_detrator_baseline) / agregado_impacto_detrator_baseline * 100,2 )
    agregado_impacto_detrator_moderada_meta = round(agregado_impacto_detrator_baseline*1.02, 2)
    agregado_impacto_detrator_moderada_variacao = round((agregado_impacto_detrator_moderada_meta - agregado_impacto_detrator_baseline) / agregado_impacto_detrator_baseline * 100,2 )
    agregado_impacto_detrator_ousada_meta = round(agregado_impacto_detrator_baseline*0.98, 2) 
    agregado_impacto_detrator_ousada_variacao = round((agregado_impacto_detrator_ousada_meta - agregado_impacto_detrator_baseline) / agregado_impacto_detrator_baseline * 100,2 )

    # media_mensal_%_impacto_detrator
    descr_media_mensal_perc_impacto_detrator = 'O indicador é a média mensal do percentual do impacto detrator da marca. É ideal para observar o desempenho mensal da percepção crítica, com o objetivo de tentar identificar desvios importantes e reduzir picos de imagem negativa no decorrer no ano'
    media_mensal_perc_impacto_detrator_baseline = round(agg_mes_nps_score['% Detratores'].mean(), 2)*100
    media_mensal_perc_impacto_detrator_conservadora_meta = min(round(media_mensal_perc_impacto_detrator_baseline +  (agg_mes_nps_score['% Detratores'].std() / 3), 2)*100, 2) 
    media_mensal_perc_impacto_detrator_conservadora_variacao = round((media_mensal_perc_impacto_detrator_conservadora_meta - media_mensal_perc_impacto_detrator_baseline) / media_mensal_perc_impacto_detrator_baseline * 100,2 )
    media_mensal_perc_impacto_detrator_moderada_meta = min(round(media_mensal_perc_impacto_detrator_baseline*1.02, 2)*100, 0.00)
    media_mensal_perc_impacto_detrator_moderada_variacao = round((media_mensal_perc_impacto_detrator_moderada_meta - media_mensal_perc_impacto_detrator_baseline) / media_mensal_perc_impacto_detrator_baseline * 100,2 )
    media_mensal_perc_impacto_detrator_ousada_meta =  min(round(media_mensal_perc_impacto_detrator_baseline -  (agg_mes_nps_score['% Detratores'].std() / 4), 2)*100, 0.0)  
    media_mensal_perc_impacto_detrator_ousada_variacao = round((media_mensal_perc_impacto_detrator_ousada_meta - media_mensal_perc_impacto_detrator_baseline) / media_mensal_perc_impacto_detrator_baseline * 100,2 )


    # agregado_de_%_detrator
    descr_agregado_perc_impacto_detrator = 'O indicador é o percentual do impacto detrator da marca no ano. É ideal para observar o total da percepção crítica acumulada no ano sobre a marca.'
    agregado_perc_impacto_detrator_baseline = round(agg_ano_nps_score['% Detratores'].mean(), 2)*100
    agregado_perc_impacto_detrator_conservadora_meta = round(agregado_perc_impacto_detrator_baseline*1.05, 2)*100
    agregado_perc_impacto_detrator_conservadora_variacao = round((agregado_perc_impacto_detrator_conservadora_meta - agregado_perc_impacto_detrator_baseline) / agregado_perc_impacto_detrator_baseline * 100,2 )
    agregado_perc_impacto_detrator_moderada_meta = round(agregado_perc_impacto_detrator_baseline*1.02, 2)*100
    agregado_perc_impacto_detrator_moderada_variacao = round((agregado_perc_impacto_detrator_moderada_meta - agregado_perc_impacto_detrator_baseline) / agregado_perc_impacto_detrator_baseline * 100,2 )
    agregado_perc_impacto_detrator_ousada_meta = round(agregado_perc_impacto_detrator_baseline*0.98, 2)*100
    agregado_perc_impacto_detrator_ousada_variacao = round((agregado_perc_impacto_detrator_ousada_meta - agregado_perc_impacto_detrator_baseline) / agregado_perc_impacto_detrator_baseline * 100,2 )

    Impacto_Detrator_dict = {'Impressão Detratora - Média mensal' :  
                                    { 'descricao' : descr_media_mensal_impacto_detrator, 
                                        'baseline' : media_mensal_impacto_detrator_baseline, 
                                        
                                        'conservadora' : {'meta' : media_mensal_impacto_detrator_conservadora_meta, 
                                                    'variacao' : media_mensal_impacto_detrator_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : media_mensal_impacto_detrator_moderada_meta, 
                                                    'variacao' : media_mensal_impacto_detrator_moderada_variacao}, 

                                    'ousada' : {'meta' : media_mensal_impacto_detrator_ousada_meta,  
                                                    'variacao' : media_mensal_impacto_detrator_ousada_variacao}}, 
                
                            'Impressão Detratora - Acumulado no ano' : {
                                    'descricao' : descr_agregado_impacto_detrator, 
                                    'baseline' : agregado_impacto_detrator_baseline, 
                                    'conservadora' : {'meta' : agregado_impacto_detrator_conservadora_meta, 
                                                    'variacao' : agregado_impacto_detrator_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : agregado_impacto_detrator_moderada_meta , 
                                                    'variacao' : agregado_impacto_detrator_moderada_variacao} , 
                
                                    'ousada' : {'meta' : agregado_impacto_detrator_ousada_meta , 
                                                    'variacao' : agregado_impacto_detrator_ousada_variacao}}, 
                    
                            'Impressão Detratora (%) - Média mensal' : {
                                    'descricao' : descr_media_mensal_perc_impacto_detrator, 
                                    'baseline' : media_mensal_perc_impacto_detrator_baseline, 
                                    'conservadora' : {'meta' : media_mensal_perc_impacto_detrator_conservadora_meta, 
                                                    'variacao' : media_mensal_perc_impacto_detrator_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : media_mensal_perc_impacto_detrator_moderada_meta , 
                                                    'variacao' : media_mensal_perc_impacto_detrator_moderada_variacao} , 
                
                                    'ousada' : {'meta' : media_mensal_perc_impacto_detrator_ousada_meta , 
                                                    'variacao' : media_mensal_perc_impacto_detrator_ousada_variacao}, 

                            'Impressão Detratora (%) - Acumulado no ano' : {
                                    'descricao' : descr_agregado_perc_impacto_detrator, 
                                    'baseline' : agregado_perc_impacto_detrator_baseline, 
                                    'conservadora' : {'meta' : agregado_perc_impacto_detrator_conservadora_meta, 
                                                    'variacao' : agregado_perc_impacto_detrator_conservadora_variacao} , 
                                    
                                    'moderada' : {'meta' : agregado_perc_impacto_detrator_moderada_meta , 
                                                    'variacao' : agregado_perc_impacto_detrator_moderada_variacao} , 
                
                                    'ousada' : {'meta' : agregado_perc_impacto_detrator_ousada_meta , 
                                                    'variacao' : agregado_perc_impacto_detrator_ousada_variacao}}, 


                }}
    
    ### PROTOGONISMO 
    ### gerando valores media_mensal_protagonismo_total; agregado_protagonismo_total; media_mensal_protagonismo_promotor; agregado_protagonismo_promotor;

    # media_mensal_protagonismo_total
    descr_media_mensal_protagonismo_total = 'Trata-se média mensal da exposição protagonista da marca nas mídias. O indicador mede a possibilidade da audiência ser impactada pela marca na leitura da publicação. \
        Desta forma, essa mensuração é importante para verificar se a mensagem sobre a sua marca tem mais, ou menos, possiblidade de ser lembrada no decorrer dos meses'
    media_mensal_protagonismo_total_baseline = round(agg_mes_protagonismo_score['protagonism_score'].mean(), 2)*100
    media_mensal_protagonismo_total_conservador_meta = min(round(media_mensal_protagonismo_total_baseline*0.98, 2), 100.00)
    media_mensal_protagonismo_total_conservador_variacao = round(media_mensal_protagonismo_total_conservador_meta - media_mensal_protagonismo_total_baseline, 2)
    media_mensal_protagonismo_total_moderado_meta = min(round(media_mensal_protagonismo_total_baseline*1.02, 2), 100.00)
    media_mensal_protagonismo_total_moderado_variacao = round(media_mensal_protagonismo_total_moderado_meta - media_mensal_protagonismo_total_baseline, 2)
    media_mensal_protagonismo_total_ousado_meta = min(round(media_mensal_protagonismo_total_baseline*1.05, 2), 100.00)
    media_mensal_protagonismo_total_ousado_variacao = round(media_mensal_protagonismo_total_ousado_meta - media_mensal_protagonismo_total_baseline, 2)

    # agregado_protagonismo_total
    descr_agregado_protagonismo_total = 'Trata-se da exposição protagonista da marca nas mídias no decorrer do ano. O indicador mede a possibilidade da audiência ser impactada pela marca na leitura da publicação. \
        Desta forma, essa mensuração é importante para verificar se a mensagem sobre a sua marca tem mais, ou menos, possiblidade de forma acumulativa no decorrer do ano'
    agregado_protagonismo_total_baseline = round(agg_ano_protagonismo_score['protagonism_score'].mean(), 2)*100
    agregado_protagonismo_total_conservador_meta = round(agregado_protagonismo_total_baseline*0.98, 2)
    agregado_protagonismo_total_conservador_variacao = round((agregado_protagonismo_total_conservador_meta - agregado_protagonismo_total_baseline), 2)
    agregado_protagonismo_total_moderador_meta = min(round(agregado_protagonismo_total_baseline*1.02, 2), 100.00)
    agregado_protagonismo_total_moderador_variacao = round((agregado_protagonismo_total_moderador_meta - agregado_protagonismo_total_baseline), 2)
    agregado_protagonismo_total_ousado_meta = min(round(agregado_protagonismo_total_baseline*1.05, 2), 100.00)
    agregado_protagonismo_total_ousado_variacao = round((agregado_protagonismo_total_ousado_meta - agregado_protagonismo_total_baseline), 2)


    # media_mensal_protagonismo_promotor
    descr_media_mensal_protagonismo_promotor = 'Trata-se média mensal da exposição protagonista promotora da marca nas mídias. O indicador mede a possibilidade da audiência ser impactada pela marca na leitura da publicação. \
        Desta forma, essa mensuração é importante para verificar se a exposição favorável sobre a sua marca tem mais, ou menos, possiblidade de ser lembrada no decorrer dos meses'
    media_mensal_protagonismo_promotor_baseline = round(agg_mes_protagonismo_promotor_score['protagonism_score'].mean(), 2)*100
    media_mensal_protagonismo_promotor_conservador_meta = min(round(media_mensal_protagonismo_promotor_baseline*0.98, 2), 100.00)
    media_mensal_protagonismo_promotor_conservador_variacao = round(media_mensal_protagonismo_promotor_conservador_meta - media_mensal_protagonismo_promotor_baseline, 2)
    media_mensal_protagonismo_promotor_moderado_meta = min(round(media_mensal_protagonismo_promotor_baseline*1.02, 2), 100.00)
    media_mensal_protagonismo_promotor_moderado_variacao = round(media_mensal_protagonismo_promotor_moderado_meta - media_mensal_protagonismo_promotor_baseline, 2)
    media_mensal_protagonismo_promotor_ousado_meta = min(round(media_mensal_protagonismo_promotor_baseline*1.05, 2), 100.00)
    media_mensal_protagonismo_promotor_ousado_variacao = round(media_mensal_protagonismo_promotor_ousado_meta - media_mensal_protagonismo_promotor_baseline, 2)


    # agregado_protagonismo_promotor
    descr_agregado_protagonismo_promotor = 'Trata-se da exposição protagonista promotora da marca nas mídias no decorrer do ano. O indicador mede a possibilidade da audiência ser impactada pela marca na leitura da publicação. \
        Desta forma, essa mensuração é importante para verificar se a mensagem favorável sobre a sua marca tem mais, ou menos, possiblidade de forma acumulativa no decorrer do ano'
    agregado_protagonismo_promotor_baseline = round(agg_ano_protagonismo_promotor_score['protagonism_score'].mean(), 2)*100
    agregado_protagonismo_promotor_conservador_meta = round(agregado_protagonismo_promotor_baseline*0.98, 2)
    agregado_protagonismo_promotor_conservador_variacao = round((agregado_protagonismo_promotor_conservador_meta - agregado_protagonismo_promotor_baseline), 2)
    agregado_protagonismo_promotor_moderado_meta = min(round(agregado_protagonismo_promotor_baseline*1.02, 2), 100.00)
    agregado_protagonismo_promotor_moderado_variacao = round((agregado_protagonismo_promotor_moderado_meta - agregado_protagonismo_promotor_baseline), 2)
    agregado_protagonismo_promotor_ousado_meta = min(round(agregado_protagonismo_promotor_baseline*1.05, 2), 100.00)
    agregado_protagonismo_promotor_ousado_variacao = round((agregado_protagonismo_promotor_ousado_meta - agregado_protagonismo_promotor_baseline), 2)


    Protagonismo_dict = {'Protagonismo - Média mensal' :  
                                    { 'descricao' : descr_media_mensal_protagonismo_total, 
                                        'baseline' : media_mensal_protagonismo_total_baseline, 
                                        
                                        'conservadora' : {'meta' : media_mensal_protagonismo_total_conservador_meta, 
                                                    'variacao' : media_mensal_protagonismo_total_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : media_mensal_protagonismo_total_moderado_meta, 
                                                    'variacao' : media_mensal_protagonismo_total_moderado_variacao}, 

                                    'ousada' : {'meta' : media_mensal_protagonismo_total_ousado_meta,  
                                                    'variacao' : media_mensal_protagonismo_total_ousado_variacao}}, 
                
                            'Protagonismo - Acumulado no ano' : {
                                    'descricao' : descr_agregado_protagonismo_total, 
                                    'baseline' : agregado_protagonismo_total_baseline, 
                                    'conservadora' : {'meta' : agregado_protagonismo_total_conservador_meta, 
                                                    'variacao' : agregado_protagonismo_total_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : agregado_protagonismo_total_moderador_meta , 
                                                    'variacao' : agregado_protagonismo_total_moderador_variacao} , 
                
                                    'ousada' : {'meta' : agregado_protagonismo_total_ousado_meta , 
                                                    'variacao' : agregado_protagonismo_total_ousado_variacao}}, 
                    
                            'Protagonismo Promotor - Média mensal' : {
                                    'descricao' : descr_media_mensal_protagonismo_promotor, 
                                    'baseline' : media_mensal_protagonismo_promotor_baseline, 
                                    'conservadora' : {'meta' : media_mensal_protagonismo_promotor_conservador_meta, 
                                                    'variacao' : media_mensal_protagonismo_promotor_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : media_mensal_protagonismo_promotor_moderado_meta , 
                                                    'variacao' : media_mensal_protagonismo_promotor_moderado_variacao} , 
                
                                    'ousada' : {'meta' : media_mensal_protagonismo_promotor_ousado_meta , 
                                                    'variacao' : media_mensal_protagonismo_promotor_ousado_variacao}, 

                            'Protagonismo Promotor - Acumulado no ano' : {
                                    'descricao' : descr_agregado_protagonismo_promotor, 
                                    'baseline' : agregado_protagonismo_promotor_baseline, 
                                    'conservadora' : {'meta' : agregado_protagonismo_promotor_conservador_meta, 
                                                    'variacao' : agregado_protagonismo_promotor_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : agregado_protagonismo_promotor_moderado_meta , 
                                                    'variacao' : agregado_protagonismo_promotor_moderado_variacao} , 
                
                                    'ousada' : {'meta' : agregado_protagonismo_promotor_ousado_meta , 
                                                    'variacao' : agregado_protagonismo_promotor_ousado_variacao}}, 


                }}

    ### FREQUÊNCIA 
    ### gerando valores frequencia_media_mensal, frequencia_media_mensal_promotora, frequencia_media_mensal_detratora
    ### frequencia_acumulada_ano, frequencia_acumulada_promotora_ano, frequencia_acumulada_detratrora_ano

    # cadencia de exposição frequencia_media_mensal
    descr_frequencia_media_mensal = 'Trata-se da frequência média diária por mês da exposição total da marca nas mídias. O indicador verifica a assuidade da ' \
    'presença da marca nas mídias por dia. Quanto mais frequente, maior a chance da empresa ser lembrada pela audiência.'
    frequencia_media_mensal_baseline = round(agg_mes_freq_score['Freq. Media por Dia'].mean(),2) 
    frequencia_media_mensal_conservador_meta = round(frequencia_media_mensal_baseline - (agg_mes_freq_score['Freq. Media por Dia'].std() / 3), 2)
    frequencia_media_mensal_conservador_variacao = round((frequencia_media_mensal_conservador_meta - frequencia_media_mensal_baseline) / frequencia_media_mensal_baseline *100, 2) 
    frequencia_media_mensal_moderado_meta = round(frequencia_media_mensal_baseline*1.02, 2)
    frequencia_media_mensal_moderado_variacao = round((frequencia_media_mensal_moderado_meta - frequencia_media_mensal_baseline) / frequencia_media_mensal_baseline *100, 2) 
    frequencia_media_mensal_ousado_meta = round(frequencia_media_mensal_baseline + (agg_mes_freq_score['Freq. Media por Dia'].std() / 3), 2)
    frequencia_media_mensal_ousado_variacao = round((frequencia_media_mensal_ousado_meta - frequencia_media_mensal_baseline) / frequencia_media_mensal_baseline *100, 2)

    # cadencia de exposição promotora frequencia_media_mensal_promotora
    descr_frequencia_media_mensal_promotora = 'Trata-se da frequência média diária promotores por mês da exposição total da marca nas mídias. O indicador verifica a assuidade da ' \
    'presença da marca nas mídias por dia. Quanto mais frequente, maior a chance da empresa ser lembrada pela audiência de forma favorável.'
    frequencia_media_mensal_promotora_baseline = round(agg_mes_freq_score['Freq. Promotora Media por Dia'].mean(),2) 
    frequencia_media_mensal_promotora_conservador_meta = round(frequencia_media_mensal_baseline - (agg_mes_freq_score['Freq. Promotora Media por Dia'].std() / 3), 2)
    frequencia_media_mensal_promotora_conservador_variacao = round((frequencia_media_mensal_promotora_conservador_meta - frequencia_media_mensal_promotora_baseline) / frequencia_media_mensal_promotora_baseline *100, 2) 
    frequencia_media_mensal_promotora_moderado_meta = round(frequencia_media_mensal_baseline*1.02, 2)
    frequencia_media_mensal_promotora_moderado_variacao = round((frequencia_media_mensal_promotora_moderado_meta - frequencia_media_mensal_promotora_baseline) / frequencia_media_mensal_promotora_baseline *100, 2) 
    frequencia_media_mensal_promotora_ousado_meta = round(frequencia_media_mensal_baseline + (agg_mes_freq_score['Freq. Promotora Media por Dia'].std() / 3), 2)
    frequencia_media_mensal_promotora_ousado_variacao = round((frequencia_media_mensal_promotora_ousado_meta - frequencia_media_mensal_promotora_baseline) / frequencia_media_mensal_promotora_baseline *100, 2) 
    
    # frequencia_acumulada_ano
    descr_frequencia_acumulada_ano = 'Trata-se da presença acumulada no ano da marca nas mídias. Quanto maior a presença, maior a chance da marca ser lembrada.'
    frequencia_acumulada_ano_baseline = round(agg_mes_freq_score['total'].sum(),0)
    frequencia_acumulada_ano_conservador_meta = round(frequencia_acumulada_ano_baseline*0.98,0)
    frequencia_acumulada_ano_conservador_variacao = round((frequencia_acumulada_ano_conservador_meta - frequencia_acumulada_ano_baseline) / frequencia_acumulada_ano_baseline  *100, 2)
    frequencia_acumulada_ano_moderador_meta = round(frequencia_acumulada_ano_baseline*1.02, 0)
    frequencia_acumulada_ano_moderador_variacao = round((frequencia_acumulada_ano_moderador_meta - frequencia_acumulada_ano_baseline) / frequencia_acumulada_ano_baseline  *100, 2)
    frequencia_acumulada_ano_ousado_meta = round(frequencia_acumulada_ano_baseline*1.05,0)
    frequencia_acumulada_ano_ousado_variacao = round((frequencia_acumulada_ano_ousado_meta - frequencia_acumulada_ano_baseline) / frequencia_acumulada_ano_baseline  *100, 2)


    # frequencia_acumulada_promotora_ano
    descr_frequencia_acumulada_promotora_ano = 'Trata-se da presença promotora acumulada no ano da marca nas mídias. Quanto maior a presença, maior a chance da marca ser lembrada de forma favorável.'
    frequencia_acumulada_promotora_ano_baseline = round(agg_mes_freq_score['Promotores'].sum(),0)
    frequencia_acumulada_promotora_ano_conservador_meta = round(frequencia_acumulada_promotora_ano_baseline*0.98,0)
    frequencia_acumulada_promotora_ano_conservador_variacao = round((frequencia_acumulada_promotora_ano_conservador_meta - frequencia_acumulada_promotora_ano_baseline) / frequencia_acumulada_promotora_ano_baseline  *100, 2)
    frequencia_acumulada_promotora_ano_moderador_meta = round(frequencia_acumulada_promotora_ano_baseline*1.02, 0)
    frequencia_acumulada_promotora_ano_moderador_variacao = round((frequencia_acumulada_promotora_ano_moderador_meta - frequencia_acumulada_promotora_ano_baseline) / frequencia_acumulada_promotora_ano_baseline  *100, 2)
    frequencia_acumulada_promotora_ano_ousado_meta = round(frequencia_acumulada_promotora_ano_baseline*1.05,0)
    frequencia_acumulada_promotora_ano_ousado_variacao = round((frequencia_acumulada_promotora_ano_ousado_meta - frequencia_acumulada_promotora_ano_baseline) / frequencia_acumulada_promotora_ano_baseline  *100, 2)


    Freq_dict = {'Cadência de Publicações - Média por dia' :  
                                    { 'descricao' : descr_frequencia_media_mensal, 
                                        'baseline' : frequencia_media_mensal_baseline, 
                                        
                                        'conservadora' : {'meta' : frequencia_media_mensal_conservador_meta, 
                                                    'variacao' : frequencia_media_mensal_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : frequencia_media_mensal_moderado_meta, 
                                                    'variacao' : frequencia_media_mensal_moderado_variacao}, 

                                    'ousada' : {'meta' : frequencia_media_mensal_ousado_meta,  
                                                    'variacao' : frequencia_media_mensal_ousado_variacao}}, 

            'Cadência de Publicações Promotora - Média por dia' :  
                                    { 'descricao' : descr_frequencia_media_mensal_promotora, 
                                        'baseline' : frequencia_media_mensal_promotora_baseline, 
                                        
                                        'conservadora' : {'meta' : frequencia_media_mensal_promotora_conservador_meta, 
                                                    'variacao' : frequencia_media_mensal_promotora_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : frequencia_media_mensal_promotora_moderado_meta, 
                                                    'variacao' : frequencia_media_mensal_promotora_moderado_variacao}, 

                                    'ousada' : {'meta' : frequencia_media_mensal_promotora_ousado_meta,  
                                                    'variacao' : frequencia_media_mensal_promotora_ousado_variacao}}, 

            'Publicações - Acumulado no ano' :  
                                    { 'descricao' : descr_frequencia_acumulada_ano, 
                                        'baseline' : frequencia_acumulada_ano_baseline, 
                                        
                                        'conservadora' : {'meta' : frequencia_acumulada_ano_conservador_meta, 
                                                    'variacao' : frequencia_acumulada_ano_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : frequencia_acumulada_ano_moderador_meta, 
                                                    'variacao' : frequencia_acumulada_ano_moderador_variacao}, 

                                    'ousada' : {'meta' : frequencia_acumulada_ano_ousado_meta,  
                                                    'variacao' : frequencia_acumulada_ano_ousado_variacao}}, 


            'Publicações Promotora - Acumulado no ano' :  
                                    { 'descricao' : descr_frequencia_acumulada_promotora_ano, 
                                        'baseline' : frequencia_acumulada_promotora_ano_baseline, 
                                        
                                        'conservadora' : {'meta' : frequencia_acumulada_promotora_ano_conservador_meta, 
                                                    'variacao' : frequencia_acumulada_promotora_ano_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : frequencia_acumulada_promotora_ano_moderador_meta, 
                                                    'variacao' : frequencia_acumulada_promotora_ano_moderador_variacao}, 

                                    'ousada' : {'meta' : frequencia_acumulada_promotora_ano_ousado_meta,  
                                                    'variacao' : frequencia_acumulada_promotora_ano_ousado_variacao}}, 


    }

    ### VALORAÇÃO
    ### gerando valores valoracao_media_mensal, valoracao_acumulada_ano

    # valoracao_media_mensal
    descr_valoracao_media_mensal = 'Trata-se do valor monetário (R$) potencial da exposição orgânica nas mídias. Esse indicador serve de referência sobre o valor monetário retornado pela exposição espontânea nas mídias'
    valoracao_media_mensal_baseline = round(agg_mes_valoration_score['valoracao'].mean(),2) 
    valoracao_media_mensal_conservador_meta = round(valoracao_media_mensal_baseline - (agg_mes_valoration_score['valoracao'].std() / 5), 2)
    valoracao_media_mensal_conservador_variacao = round((valoracao_media_mensal_conservador_meta - valoracao_media_mensal_baseline) / valoracao_media_mensal_baseline *100, 2) 
    valoracao_media_mensal_moderado_meta = round(valoracao_media_mensal_baseline*1.02, 2)
    valoracao_media_mensal_moderado_variacao = round((valoracao_media_mensal_moderado_meta - valoracao_media_mensal_baseline) / valoracao_media_mensal_baseline *100, 2) 
    valoracao_media_mensal_ousado_meta = round(valoracao_media_mensal_baseline + (agg_mes_valoration_score['valoracao'].std() / 5), 2)
    valoracao_media_mensal_ousado_variacao = round((valoracao_media_mensal_ousado_meta - valoracao_media_mensal_baseline) / valoracao_media_mensal_baseline *100, 2) 

    #valoracao_acumulada_ano

    descr_valoracao_acumulada_ano = 'Trata-se do valor monetário (R$) potencial da exposição orgânica nas mídias. Esse indicador serve de referência sobre o valor monetário retornado pela exposição espontânea nas mídias'
    valoracao_acumulada_ano_baseline = round(agg_ano_valoration_score['valoracao'].mean(), 2) 
    valoracao_media_ano_conservador_meta = round(valoracao_acumulada_ano_baseline*0.98, 2)
    valoracao_media_ano_conservador_variacao = round((valoracao_media_ano_conservador_meta - valoracao_acumulada_ano_baseline) / valoracao_acumulada_ano_baseline *100, 2) 
    valoracao_media_ano_moderado_meta = round(valoracao_acumulada_ano_baseline*1.02, 2)
    valoracao_media_ano_moderado_variacao = round((valoracao_media_ano_moderado_meta - valoracao_acumulada_ano_baseline) / valoracao_acumulada_ano_baseline *100, 2) 
    valoracao_media_ano_ousado_meta = round(valoracao_acumulada_ano_baseline*1.05, 2)
    valoracao_media_ano_ousado_variacao = round((valoracao_media_ano_ousado_meta - valoracao_acumulada_ano_baseline) / valoracao_acumulada_ano_baseline *100, 2) 

    Valoracao_dict = {'Valoração - Média mensal' :  
                                    { 'descricao' : descr_valoracao_media_mensal, 
                                        'baseline' : valoracao_media_mensal_baseline, 
                                        
                                        'conservadora' : {'meta' : valoracao_media_mensal_conservador_meta, 
                                                    'variacao' : valoracao_media_mensal_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : valoracao_media_mensal_moderado_meta, 
                                                    'variacao' : valoracao_media_mensal_moderado_variacao}, 

                                    'ousada' : {'meta' : valoracao_media_mensal_ousado_meta,  
                                                    'variacao' : valoracao_media_mensal_ousado_variacao}}, 

                    'Valoração - Acumulado no ano' :  
                                    { 'descricao' : descr_valoracao_acumulada_ano, 
                                        'baseline' : valoracao_acumulada_ano_baseline, 
                                        
                                        'conservadora' : {'meta' : valoracao_media_ano_conservador_meta, 
                                                    'variacao' : valoracao_media_ano_conservador_variacao} , 
                                    
                                    'moderada' : {'meta' : valoracao_media_ano_moderado_meta, 
                                                    'variacao' : valoracao_media_ano_moderado_variacao}, 

                                    'ousada' : {'meta' : valoracao_media_ano_ousado_meta,  
                                                    'variacao' : valoracao_media_ano_ousado_variacao}},                                   
    }

 


    ###################### Dicionário Final ################

    goals_dict = {'NPS' : NPS_dict,
                  'Impressão Promotora' : Impacto_Promotor_dict,
                  'Impressão Detratora' : Impacto_Detrator_dict,
                  'Protagonismo' :  Protagonismo_dict,
                  'Frequência' : Freq_dict,
                  'Valoração' : Valoracao_dict,
                  'Contr. ao NPS' : contr_dict


    }

    return goals_dict



def gen_goals_calculations(path_dataframe: str) -> Dict:
    dataview = gen_dataviews(path_dataframe)
    dict_goals_calculations = goals_calculations(dataview)

    return dict_goals_calculations