import json
import pandas as pd
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
import json
import os

def load_json_(filename, folder_name):
    try:
        with open(os.path.join(folder_name, filename), 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def gen_goals(client_name):

    brands_info = load_json_(f'brand_info.json', f'projects_files\\{client_name}')
    brand_dict= load_json_(f'all_data.json', f'projects_files\\{client_name}')
    brand_name = brand_dict['clients'][0]
    
    media_analysis_info = load_json_(f'media_analysis.json', f'projects_files\\{client_name}')
    communicable_facts = load_json_(f'communicable_facts.json', f'projects_files\\{client_name}')
    objetivos =  load_json_(f'context.json', f'projects_files\\{client_name}')
    oportunidades =  load_json_(f'copportunities.json', f'projects_files\\{client_name}')
    riscos =  load_json_(f'risks.json', f'projects_files\\{client_name}')
    
    # knowledge_base
    business_objectives = pd.read_csv(r'knowledge_base\business_objectives.csv')
    communication_strategies = pd.read_csv(r'knowledge_base\communication_strategies.csv')
    index_description = pd.read_csv(r'knowledge_base\cortex_index_description.csv')
    index_description = pd.read_csv(r'knowledge_base\cortex_index_description_type.csv')
    index_description_dict = (
            index_description
            .set_index("Indicadores")[["Tipo", "descrição"]]
            .to_dict(orient="index")
        )
    index_list = index_description['Indicadores'].to_list() ### lista de indicadores que precisam ser retornados
    audience_impact_df = pd.read_csv(r'knowledge_base\comun_discp.csv')
    audience_impact_description = (
        audience_impact_df[:12]
        .set_index(['Objetivo de comunicação'])[['Descrição']]
        .to_dict(orient="index")
    )
    audience_impact_list = audience_impact_df[:12]['Objetivo de comunicação'].to_list()

    analysis_template = ''' 
Você é um analista de negócios especializado em comunicação institucional e relações públicas.
O seu público são executivos de comunicação que precisam definir seus objetivos, metas e kpis, a partir de informações sobre o {brand_name}
Seu objetivo é definir o objetivo de comunicação, sendo perseguido por três indicadores — que estão  dentro desta lista {index_list}, 
aos quais cada um é associado a um tipo de impacto na audiência que está dentro desta lista {audience_impact_list}. 

As ações de um especialista em comunicação institucional tendem a ser relacionadas ao ganho de lembrança da marca e melhora de percepção e reputação.
Como contexto para a construção dos objetivos, temos as informações gerais da marca {brands_info} e o diagnóstico de imagem e reputação {media_analysis_info}.

As informações estratégicas, de acordo com o cliente, são  os objetivos de comunicação {objetivos}, as oportunidades percebidas pelo cliente {oportunidades} e os riscos de imagem {riscos}.

Os objetivos devem ser baseados nas informações dos fatos comunicáveis, que já foram revisados pelos cliente: {communicable_facts}. 
Para a definição dos objetivos, é importante saber o que são {business_objectives} e {communication_strategies}. 

Para sugerir os indicadores, é importante saber quais são suas definições {index_description_dict} e levar em consideração como se associam aos objetivos definidos.

E para o impacto na audiência tem de saber quais são suas definições {audience_impact_description} e levar em consideração como se associam aos objetivos definidos.

outputs
# Gere no mínimo 3 objetivos
# Retorne APENAS um JSON válido (sem markdown, sem texto fora do JSON).
# Estrutura: lista de dicionários, cada um com:
# "objectives": "texto...",
# "index": ["index1", "index2", "index3"],
# "audience impact": ["impacto1", "impacto2", "impacto3"]
'''

    analysis_prompt_template = PromptTemplate(
        input_variables=[
            'brand_name', 'index_list', 'audience_impact_list', 'brands_info',
            'media_analysis_info', 'objetivos', 'oportunidades', 'riscos',
            'communicable_facts', 'business_objectives', 'communication_strategies',
            'index_description_dict', 'audience_impact_description'
        ],
        template=analysis_template,
    )

    llm = ChatOpenAI(
        temperature=0.70,
        model_name='gpt-4o',
        model_kwargs={"response_format": {"type": "json_object"}}
    )

    chain = analysis_prompt_template | llm

    response = chain.invoke({
        "brand_name": brand_name,
        "index_list": index_list,
        "audience_impact_list": audience_impact_list,
        "brands_info": brands_info,
        "media_analysis_info": media_analysis_info,
        "objetivos": objetivos,
        "oportunidades": oportunidades,
        "riscos": riscos,
        "communicable_facts": communicable_facts,
        "business_objectives": business_objectives.to_dict(orient="records"),
        "communication_strategies": communication_strategies.to_dict(orient="records"),
        "index_description_dict": index_description_dict,
        "audience_impact_description": audience_impact_description,
    })

    # Agora costuma vir JSON puro
    json_outputs = json.loads(response.content)
    return json_outputs
