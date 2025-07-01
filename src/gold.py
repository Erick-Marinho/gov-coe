# src/gold.py

import pandas as pd
import numpy as np
from pathlib import Path

def processar_camada_gold():
    """
    Gera tabelas analíticas finais baseadas no contexto do notebook de auditoria.
    """
    print("🏆 Iniciando processamento da Camada Gold...")

    # Caminhos
    silver_path = Path("./data/silver")
    gold_path = Path("./data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)

    try:
        # Carregar dados da Silver
        print("📖 Carregando dados da camada Silver...")
        df_apps_completo = pd.read_csv(silver_path / "apps_com_metricas.csv")
        df_ambiente = pd.read_csv(silver_path / "resumo_por_ambiente.csv")
        df_alta_adocao = pd.read_csv(silver_path / "apps_alta_adocao.csv")
        df_metricas_uso = pd.read_csv(silver_path / "metricas_uso_auditoria.csv")
        
        print(f"✅ Apps com métricas completas: {len(df_apps_completo)} registros")
        print(f"✅ Apps de alta adoção: {len(df_alta_adocao)} registros")
        print(f"✅ Métricas de uso: {len(df_metricas_uso)} registros")

    except Exception as e:
        print(f"❌ Erro ao carregar dados: {e}")
        return

    # 1. TABELA PRINCIPAL: APPS DE ALTA ADOÇÃO (como no notebook)
    print("🏆 Gerando tabela de Apps de Alta Adoção...")
    
    if len(df_alta_adocao) > 0:
        colunas_alta_adocao = [
            'Nome_App', 'Nome_Ambiente',
            'Nome_Criador', 'total_proprietarios',
            'usuarios_unicos', 'sessoes_totais', 'Data_Ultimo_Acesso'
        ]
        
        # Verificar quais colunas existem
        colunas_existentes = [col for col in colunas_alta_adocao if col in df_alta_adocao.columns]
        
        tabela_apps_alta_adocao = df_alta_adocao[colunas_existentes].copy()
        
        # Renomear colunas (como no notebook)
        mapeamento_colunas = {
            'Nome_App': 'Nome do App',
            'Nome_Ambiente': 'Ambiente',
            'Nome_Criador': 'Proprietário Principal',
            'total_proprietarios': 'Total de Proprietários/Editores',
            'usuarios_unicos': 'Usuários Únicos',
            'sessoes_totais': 'Total de Sessões',
            'Data_Ultimo_Acesso': 'Último Acesso'
        }
        
        # Filter mapping to only existing columns
        mapeamento_filtrado = {k: v for k, v in mapeamento_colunas.items() if k in tabela_apps_alta_adocao.columns}
        if len(tabela_apps_alta_adocao) > 0 and mapeamento_filtrado:
            new_columns = [mapeamento_filtrado.get(col, col) for col in tabela_apps_alta_adocao.columns]
            tabela_apps_alta_adocao.columns = new_columns
    else:
        print("⚠️ Nenhum app de alta adoção encontrado")
        tabela_apps_alta_adocao = pd.DataFrame()

    # 2. TABELA DE DIMENSÃO MACRO (como no notebook)
    print("📊 Gerando tabela de Dimensão Macro...")
    
    total_apps_ambiente = len(df_apps_completo)
    apps_com_uso = len(df_metricas_uso)
    apps_prioritarios = len(df_alta_adocao)
    
    tabela_dimensao_macro = pd.DataFrame({
        'Etapa do Funil de Governança': [
            '1. Total de Apps no Ambiente',
            '2. Apps com Registro de Uso (Universo Relevante)',
            '3. Apps de Alto Impacto para Cadastro (Universo Prioritário)'
        ],
        'Quantidade de Aplicativos': [
            total_apps_ambiente,
            apps_com_uso,
            apps_prioritarios
        ]
    })
    
    # Adicionar percentual
    tabela_dimensao_macro['% em Relação ao Total'] = (
        tabela_dimensao_macro['Quantidade de Aplicativos'] / total_apps_ambiente * 100
    ).round(1).astype(str) + '%'

    # 3. RANKING DE APPS POR USUÁRIOS ÚNICOS
    print("👥 Gerando ranking por Usuários Únicos...")
    
    if 'usuarios_unicos' in df_apps_completo.columns:
        ranking_usuarios = df_apps_completo.nlargest(50, 'usuarios_unicos')[
            ['Nome_App', 'Nome_Criador', 'usuarios_unicos', 
             'sessoes_totais', 'Nome_Ambiente']
        ].copy()
        
        ranking_usuarios.columns = [
            'Nome do App', 'Proprietário', 'Usuários Únicos', 
            'Total de Sessões', 'Ambiente'
        ]
    else:
        ranking_usuarios = pd.DataFrame()

    # 4. ANÁLISE POR AMBIENTE
    print("🌍 Gerando análise por Ambiente...")
    
    analise_ambiente = df_ambiente.copy()
    if len(analise_ambiente) > 0:
        analise_ambiente = analise_ambiente.sort_values('total_usuarios_unicos', ascending=False)

    # 5. TOP PROPRIETÁRIOS (baseado no número de apps e usuários)
    print("👑 Gerando ranking de Proprietários...")
    
    if 'Nome_Criador' in df_apps_completo.columns:
        top_proprietarios = df_apps_completo.groupby('Nome_Criador').agg({
            'ID_App': 'count',
            'usuarios_unicos': 'sum',
            'sessoes_totais': 'sum'
        }).reset_index()
        
        top_proprietarios.columns = [
            'Proprietário', 'Total de Apps', 'Total de Usuários', 'Total de Sessões'
        ]
        top_proprietarios = top_proprietarios.sort_values('Total de Usuários', ascending=False).head(30)
    else:
        top_proprietarios = pd.DataFrame()

    # 6. MÉTRICAS EXECUTIVAS (KPIs principais)
    print("📈 Calculando métricas executivas...")
    
    total_usuarios_unicos = df_apps_completo['usuarios_unicos'].sum() if 'usuarios_unicos' in df_apps_completo.columns else 0
    total_sessoes = df_apps_completo['sessoes_totais'].sum() if 'sessoes_totais' in df_apps_completo.columns else 0
    apps_com_uso_real = len(df_apps_completo[df_apps_completo['usuarios_unicos'] > 0]) if 'usuarios_unicos' in df_apps_completo.columns else 0
    
    metricas_executivas = pd.DataFrame({
        'KPI': [
            'Total de Apps no Ambiente',
            'Apps com Registro de Uso',
            'Apps de Alto Impacto (Prioritários)',
            'Total de Usuários Únicos',
            'Total de Sessões Registradas',
            'Taxa de Adoção (%)',
            'Número de Ambientes Ativos'
        ],
        'Valor': [
            total_apps_ambiente,
            apps_com_uso,
            apps_prioritarios,
            total_usuarios_unicos,
            total_sessoes,
            round((apps_com_uso / total_apps_ambiente * 100), 1) if total_apps_ambiente > 0 else 0,
            len(df_ambiente[df_ambiente['total_apps'] > 0]) if 'total_apps' in df_ambiente.columns else 0
        ]
    })

    # 7. SALVAR TODAS AS TABELAS
    print("💾 Salvando tabelas da camada Gold...")
    
    tabelas = {
        'apps_alta_adocao_final': tabela_apps_alta_adocao,
        'dimensao_macro_governanca': tabela_dimensao_macro,
        'ranking_usuarios_unicos': ranking_usuarios,
        'analise_por_ambiente': analise_ambiente,
        'top_proprietarios': top_proprietarios,
        'metricas_executivas_kpis': metricas_executivas
    }
    
    results = {}
    for nome, df in tabelas.items():
        try:
            if len(df) > 0:
                df.to_csv(gold_path / f"{nome}.csv", index=False, encoding='utf-8')
                print(f"✅ {nome}: {len(df)} registros salvos")
                results[nome] = len(df)
            else:
                print(f"⚠️ {nome}: tabela vazia, não salva")
                results[nome] = 0
        except Exception as e:
            print(f"❌ Erro ao salvar {nome}: {e}")

    print("\n🎉 Camada Gold processada com sucesso!")
    print(f"📁 Dados salvos em: {gold_path}")
    
    return results