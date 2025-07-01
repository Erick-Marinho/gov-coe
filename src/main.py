from bronze import processar_camada_bronze
from silver import processar_camada_silver
from gold import processar_camada_gold

def main():
    """
    Função principal que orquestra a execução do pipeline usando apenas Pandas.
    """
    print("🚀 Inicializando pipeline com nomes amigáveis para Power BI...")
    
    try:
        # Camada Bronze
        print("\n" + "="*60)
        bronze_results = processar_camada_bronze()
        
        if not bronze_results:
            print("❌ Falha na camada Bronze. Interrompendo pipeline.")
            return
        
        # Camada Silver (ATUALIZADA COM NOMES AMIGÁVEIS)
        print("\n" + "="*60)
        silver_results = processar_camada_silver()
        
        if not silver_results:
            print("❌ Falha na camada Silver. Interrompendo pipeline.")
            return
            
        # Camada Gold
        print("\n" + "="*60)
        gold_results = processar_camada_gold()
        
        # Resumo final
        print("\n" + "="*60)
        print("🎉 PIPELINE COMPLETO COM SUCESSO!")
        print("="*60)
        print("\n📊 RESUMO FINAL:")
        print(f"Bronze: {sum(bronze_results.values())} registros processados")
        if silver_results:
            print(f"Silver: {sum(silver_results.values())} registros processados") 
        if gold_results:
            print(f"Gold: {sum(gold_results.values())} tabelas analíticas geradas")
            
        print("\n📁 DADOS DISPONÍVEIS:")
        print("  ./data/bronze/ - Dados brutos processados")
        print("  ./data/silver/ - ⭐ APPS_COM_METRICAS.CSV (PRINCIPAL PARA POWER BI)")
        print("  ./data/gold/ - Tabelas analíticas pré-calculadas")
        
        print("\n💡 DICA: Use apps_com_metricas.csv no Power BI com campos renomeados!")
        
    except Exception as e:
        print(f"❌ Erro crítico no pipeline: {e}")

if __name__ == "__main__":
    main()
