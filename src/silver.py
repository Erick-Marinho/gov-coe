# src/silver.py

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
import sys

# Ignorar avisos de Pandas
warnings.filterwarnings("ignore")

# Configuração dos caminhos
CURRENT_DIR = Path(__file__).parent
bronze_path = CURRENT_DIR / "data" / "bronze"
silver_path = CURRENT_DIR / "data" / "silver"

def processar_camada_silver(use_friendly_names=False):
    """Combina dados da camada bronze, aplica lógicas de negócio e salva na camada silver."""
    try:
        # 1. CARREGAR E FILTRAR DADOS INICIAIS
        print("📖 Carregando dados da camada Bronze...")
        df_apps = pd.read_csv(bronze_path / "apps.csv", index_col=False, dtype=str)
        print(f"Apps carregados: {df_apps.shape[0]} registros")
        
        # Filtrar apps não deletados e não SharePointFormApp logo no início
        print("🔍 Aplicando filtros iniciais...")
        df_apps['admin_appdeleted'] = df_apps['admin_appdeleted'].astype(str).str.lower()
        df_apps = df_apps[~df_apps['admin_appdeleted'].isin(['true', '1', 'yes'])]
        df_apps = df_apps[df_apps['admin_powerappstype'] != '597910003']  # SharePointFormApp
        print(f"Apps após filtros: {df_apps.shape[0]} registros")

        df_ambientes = pd.read_csv(bronze_path / "ambientes.csv", index_col=False, dtype=str)
        print(f"Ambientes carregados: {df_ambientes.shape[0]} registros")
        df_auditoria = pd.read_csv(bronze_path / "auditoria.csv", index_col=False, dtype=str)
        print(f"Auditoria carregada: {df_auditoria.shape[0]} registros")

        # 1. CÁLCULO DE MÉTRICAS DE USO
        print("Calculando métricas de uso a partir do log de auditoria...")
        # Usar as colunas corretas do log de auditoria: 'App ID' e 'User UPN'
        df_metricas = df_auditoria.groupby('App ID').agg(
            usuarios_unicos=('User UPN', 'nunique'),
            sessoes_totais=('App ID', 'size')
        ).reset_index()

        # Renomear a coluna de ID para corresponder ao DataFrame de apps para a junção
        df_metricas.rename(columns={'App ID': 'admin_appinternalname'}, inplace=True)
        print(f"Métricas calculadas para {df_metricas.shape[0]} apps únicos")

        # 2. COMBINAÇÃO DE DADOS PRINCIPAIS
        print("Combinando apps com métricas de uso...")

        # Garantir que df_apps é um DataFrame
        df_apps = pd.DataFrame(df_apps)
        
        if 'admin_appid' in df_apps.columns:
            df_apps = df_apps.rename(columns={'admin_appid': 'admin_appinternalname'})  # type: ignore

        df_apps_com_metricas = pd.merge(df_apps, df_metricas, on='admin_appinternalname', how='left')

        # Salvar checkpoint para Power BI antes de adicionar nomes de ambientes
        df_apps.to_csv(silver_path / "apps_com_metricas_originais.csv", index=False)

        # 2.1 ADIÇÃO DE NOMES AMIGÁVEIS DE AMBIENTE (MÉTODO ROBUSTO)
        # Limpar nomes de colunas para remover espaços em branco ocultos
        df_ambientes.columns = df_ambientes.columns.str.strip()

        # CORREÇÃO DEFINITIVA: Limpar e padronizar as CHAVES de junção.
        # 1. Remover espaços em branco de ambas as chaves.
        df_apps_com_metricas['admin_appenvironmentid'] = df_apps_com_metricas['admin_appenvironmentid'].str.strip()
        df_ambientes['admin_environmentid'] = df_ambientes['admin_environmentid'].str.strip()
        
        # 2. Remover o prefixo "Default-" dos IDs no DataFrame de aplicativos.
        # Sua observação está correta: este prefixo impede a junção correta dos dados.
        df_apps_com_metricas['admin_appenvironmentid'] = df_apps_com_metricas['admin_appenvironmentid'].str.replace(r'^Default-', '', regex=True)

        # 2.2 ADIÇÃO DO E-MAIL DOS PROPRIETÁRIOS
        print("📧 Adicionando e-mails dos proprietários...")
        
        # Carregar dados dos usuários da camada Bronze
        df_usuarios = pd.read_csv(bronze_path / "usuarios.csv", index_col=False, dtype=str)
        
        # Fazer junção para obter o e-mail do proprietário
        df_apps_com_proprietarios = pd.merge(
            df_apps_com_metricas,
            df_usuarios[['admin_recordguidasstring', 'admin_useremail', 'admin_userprincipalname']],
            left_on='admin_appowner.admin_recordguidasstring',
            right_on='admin_recordguidasstring',
            how='left',
            suffixes=('', '_proprietario')
        )
        
        # Usar admin_userprincipalname como e-mail principal, fallback para admin_useremail
        df_apps_com_proprietarios['admin_appownerupn'] = df_apps_com_proprietarios['admin_userprincipalname'].fillna(
            df_apps_com_proprietarios['admin_useremail']
        )

        # Restaurar o método pd.merge para ambientes
        df_apps_completo = pd.merge(
            df_apps_com_proprietarios,
            df_ambientes[['admin_environmentid', 'admin_displayname']],
            left_on='admin_appenvironmentid',
            right_on='admin_environmentid',
            how='left',
            suffixes=('_app', '_ambiente')
        )

        # Preencher nomes de ambiente ausentes com o ID do ambiente como fallback
        df_apps_completo['admin_displayname_ambiente'].fillna(df_apps_completo['admin_appenvironmentid'], inplace=True)
        
        # 3. MAPEAMENTO E LIMPEZA DE DADOS
        # 3.1 APLICAR FILTRO: REMOVER SHAREPOINTFORMAPP
        print("🚫 Removendo SharePointFormApp (regra de negócio)...")
        
        # Aplicar filtro diretamente no DataFrame final antes de salvar
        df_apps_completo = df_apps_completo[df_apps_completo['admin_powerappstype'] != '597910003']  # SharePointFormApp
        print(f"Registros após remoção de SharePointFormApp: {df_apps_completo.shape[0]}")

        # Verificar tipos únicos para debug
        print("Tipos de apps restantes:", np.unique(df_apps_completo['admin_powerappstype']))

        # 3.2 MAPEAMENTO DE NOMES AMIGÁVEIS E PREENCHIMENTO DE NULOS
        print("🔄 Mapeando nomes de colunas e tratando valores nulos...")
        mapeamento_nomes = {
            'admin_appinternalname': 'ID_App',
            'admin_displayname_app': 'Nome_App',
            'admin_appownerdisplayname': 'Nome_Criador',
            'admin_appownerupn': 'Email_Proprietario_App',
            'admin_displayname_ambiente': 'Nome_Ambiente',
            'admin_appenvironmentid': 'ID_Ambiente',
            'admin_appcreatedon': 'Data_Criacao_App',
            'admin_appmodifiedon': 'Data_Modificacao_App',
            'admin_applastlaunchedon': 'Data_Ultimo_Acesso',
            'admin_appsharedusers': 'Usuarios_Compartilhados',
            'admin_appsharedwithtenant': 'Compartilhado_Tenant',
            'admin_appsharedgroups': 'Compartilhado_Grupos',
            'admin_appcomplexityscore': 'Score_Complexidade',
            'admin_appsharededitors': 'Total_Editores',
            'admin_appowner': 'ID_Proprietario',
            'admin_appownerprincipaltype': 'Email_Proprietario',
            'admin_powerappstype': 'Tipo_App' 
        }
        
        df_apps_completo = df_apps_completo.rename(columns=mapeamento_nomes) # type: ignore
        
        # CORREÇÃO: Converter colunas de string para numéricas de forma robusta.
        # Primeiro para um tipo numérico geral, depois preencher NaNs e, finalmente, para o tipo final (int/bool).
        df_apps_completo['usuarios_unicos'] = pd.to_numeric(df_apps_completo['usuarios_unicos'], errors='coerce').fillna(0).astype(int) # type: ignore
        df_apps_completo['sessoes_totais'] = pd.to_numeric(df_apps_completo['sessoes_totais'], errors='coerce').fillna(0).astype(int) # type: ignore
        df_apps_completo['Usuarios_Compartilhados'] = pd.to_numeric(df_apps_completo['Usuarios_Compartilhados'], errors='coerce').fillna(0).astype(int) # type: ignore
        df_apps_completo['Total_Editores'] = pd.to_numeric(df_apps_completo['Total_Editores'], errors='coerce').fillna(0).astype(int) # type: ignore
        df_apps_completo['Compartilhado_Grupos'] = pd.to_numeric(df_apps_completo['Compartilhado_Grupos'], errors='coerce').fillna(0).astype(int) # type: ignore
        df_apps_completo['Score_Complexidade'] = pd.to_numeric(df_apps_completo['Score_Complexidade'], errors='coerce').fillna(0) # type: ignore
        
        # Para a coluna booleana, uma conversão segura é verificar a string 'true'.
        df_apps_completo['Compartilhado_Tenant'] = df_apps_completo['Compartilhado_Tenant'].str.lower() == 'true'
        
        # 3.3 REGRA DE CLASSIFICAÇÃO: PRODUTIVIDADE PESSOAL
        print("📱 Aplicando regra de classificação: Produtividade Pessoal...")
        df_apps_completo['Produtividade_Pessoal'] = df_apps_completo['Usuarios_Compartilhados'] < 10
        print(f"Apps classificados como Produtividade Pessoal: {df_apps_completo['Produtividade_Pessoal'].sum()}")
        
        # Garantir que as colunas de data sejam do tipo datetime
        colunas_data = ['Data_Criacao_App', 'Data_Modificacao_App', 'Data_Ultimo_Acesso']
        for col in colunas_data:
            if col in df_apps_completo.columns:
                df_apps_completo[col] = pd.to_datetime(df_apps_completo[col], errors='coerce')
        
        # 4. FILTRO DE ALTA ADOÇÃO
        print("📊 Aplicando filtro de alta adoção...")
        df_apps_completo['total_proprietarios'] = 1 + df_apps_completo['Total_Editores']
        
        # Debug: verificar quais tipos ainda existem
        print("Debug - Tipos de apps antes do filtro final:")
        print(pd.Series(df_apps_completo['Tipo_App']).value_counts())
        
        # Aplicar filtros: alta adoção E remover SharePointFormApp (597910002 e 597910003)
        regra_filtro = (df_apps_completo['usuarios_unicos'] > df_apps_completo['total_proprietarios']) & \
                      (~df_apps_completo['Tipo_App'].isin(['597910002', '597910003']))  # Formulários
        df_alta_adocao = df_apps_completo[regra_filtro].copy()
        print(f"Apps de alta adoção encontrados: {df_alta_adocao.shape[0]}")
        
        # Debug: verificar tipos restantes
        print("Debug - Tipos finais:")
        print(pd.Series(df_alta_adocao['Tipo_App']).value_counts())
        
        # 5. DEFINIÇÃO DAS COLUNAS ESSENCIAIS PARA EXPORTAÇÃO
        campos_essenciais = [
            'ID_App', 'Nome_App', 'Nome_Criador', 'Email_Proprietario_App', 
            'ID_Ambiente', 'Nome_Ambiente', 'Data_Criacao_App', 'Data_Modificacao_App', 
            'Data_Ultimo_Acesso', 'usuarios_unicos', 'sessoes_totais', 'Usuarios_Compartilhados', 
            'Compartilhado_Tenant', 'Compartilhado_Grupos', 'Score_Complexidade', 'total_proprietarios',
            'Tipo_App',  # Adicionado para manter o tipo
            'Produtividade_Pessoal'  # Nova regra de classificação
        ]

        # Filtrar SharePointFormApp antes de criar o DataFrame final
        df_alta_adocao = pd.DataFrame(df_alta_adocao)
        df_alta_adocao = df_alta_adocao[df_alta_adocao['Tipo_App'] != '597910003']
        
        # Usar df_alta_adocao já filtrado
        colunas_finais = [col for col in campos_essenciais if col in df_alta_adocao.columns]
        df_power_bi = pd.DataFrame(df_alta_adocao[colunas_finais])

        # 6. EXPORTAÇÃO DOS ARQUIVOS FINAIS
        # Tabela principal para Power BI com apps de alta adoção
        caminho_power_bi = silver_path / "apps_com_metricas.csv"
        df_power_bi.to_csv(caminho_power_bi, index=False)
        print(f"✅ Tabela para Power BI salva com {df_power_bi.shape[0]} registros em {caminho_power_bi}")

        # 7. RESUMO POR AMBIENTE
        print("🏢 Criando resumo por ambiente...")
        
        resumo_ambiente = df_power_bi.groupby(['ID_Ambiente', 'Nome_Ambiente']).agg(
            total_apps=('ID_App', 'count'),
            total_usuarios_unicos=('usuarios_unicos', 'sum'),
            total_sessoes=('sessoes_totais', 'sum')
        ).reset_index()
        
        caminho_resumo = silver_path / "resumo_por_ambiente.csv"
        resumo_ambiente.to_csv(caminho_resumo, index=False)
        print(f"✅ Resumo por ambiente salvo com {resumo_ambiente.shape[0]} registros em {caminho_resumo}")

        print("\nCamada Silver processada com sucesso!")
        # Retorna um dicionário com as contagens para o resumo final
        return {
            "apps_com_metricas": df_power_bi.shape[0],
            "resumo_por_ambiente": resumo_ambiente.shape[0]
        }

    except FileNotFoundError as e:
        print(f"❌ Erro: Arquivo não encontrado na camada Bronze. Detalhes: {e}")
        return {}  # Retornar dicionário vazio em caso de falha
    except KeyError as e:
        print(f"❌ Erro crítico no pipeline: {e}")
        return {}  # Retornar dicionário vazio em caso de falha
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado na camada Silver: {e}")
        return {}  # Retornar dicionário vazio em caso de falha

if __name__ == '__main__':
    processar_camada_silver(use_friendly_names=True)