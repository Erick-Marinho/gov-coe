import pandas as pd
from pathlib import Path

def processar_camada_bronze():
    """
    Lê os dados brutos dos CSVs incluindo log de auditoria.
    """
    print("Iniciando processamento da Camada Bronze (incluindo auditoria)...")

    # Criar diretório de saída
    base_path = Path("./data/bronze")
    base_path.mkdir(parents=True, exist_ok=True)

    # Dicionário com os caminhos dos datasets
    fontes_de_dados = {
        "apps": "../datasets/admin_apps.csv",
        "ambientes": "../datasets/admin_environments.csv",
        "auditoria": "../datasets/admin_auditlog.csv",
        "usuarios": "../datasets/admin_powerplatformusers.csv"
    }

    dados_processados = {}

    for nome, caminho in fontes_de_dados.items():
        print(f"Lendo {nome} de {caminho}...")
        try:
            # Ler CSV com Pandas
            df = pd.read_csv(caminho, encoding='utf-8')
            print(f"{nome}: {len(df)} registros, {len(df.columns)} colunas")
            
            # Salvar como CSV
            output_path = base_path / f"{nome}.csv"
            df.to_csv(output_path, index=False, encoding='utf-8', header=True)
            print(f"Salvo: {output_path}")
            
            dados_processados[nome] = len(df)
            
        except Exception as e:
            print(f"Erro ao processar {nome}: {e}")

    print("\nCamada Bronze processada com sucesso!")
    print(f"Dados salvos em: {base_path}")
    print("\nResumo dos dados processados:")
    for nome, count in dados_processados.items():
        print(f"  - {nome}: {count} registros")

    return dados_processados

if __name__ == "__main__":
    processar_camada_bronze()