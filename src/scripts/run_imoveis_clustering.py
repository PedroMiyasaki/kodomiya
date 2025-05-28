def main() -> None:
    # Imports gerais
    import pandas as pd
    import numpy as np
    from scipy import stats
    import duckdb
    from datetime import datetime

    # Importar k-means e silhouete score
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import MinMaxScaler

    # Warnings
    import warnings
    warnings.filterwarnings("ignore")

    # Excel
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    from openpyxl.styles.borders import Border, Side
    from openpyxl.utils.dataframe import dataframe_to_rows

    # Instanciar função de achar clusters
    def find_best_n_of_clusters(dataframe, columns, cluster_range):
        """
        Perform clustering using KMeans and find the best number of clusters based on silhouette score.

        Parameters:
        - dataframe: DataFrame containing the dataframe for clustering.
        - columns: columns in the DataFrame to use for clustering.
        - cluster_range: Range of cluster numbers to consider.

        Returns:
        - best_clusters: Number of clusters that yield the highest silhouette score.
        - cluster_labels: Cluster labels assigned by KMeans.
        """

        # Extract the feature for clustering
        features = dataframe[columns]

        # Scale feature befores clustering
        mms = MinMaxScaler()
        features = mms.fit_transform(dataframe[columns])

        # Initialize variables to store the best silhouette score and corresponding number of clusters + history of seacrh
        best_score = -1
        best_clusters = 0
        cluster_labels = None
        n_clusters_history = {"n_clusters": [], "score": []}

        # Iterate over the specified range of cluster numbers
        for n_clusters in cluster_range:
            # Veririficar se o número de cluster testados da iteração é maior que o numero de amostras
            if n_clusters >= len(features):
                break # Se for, quebre o loop

            # Create a KMeans model with the current number of clusters
            kmeans = KMeans(n_clusters=n_clusters, random_state=32, n_init="auto")
            
            # Fit the model and get cluster labels
            labels = kmeans.fit_predict(features)
            
            # Calculate silhouette score
            score = silhouette_score(features, labels)

            # Save current atempt at n_clusters_history
            n_clusters_history["n_clusters"].append(n_clusters)
            n_clusters_history["score"].append(score)
            
            # Update best score and number of clusters if the current result is better
            if score > best_score:
                best_score = score
                best_clusters = n_clusters
                cluster_labels = labels

        print(f"Best Number of Clusters: {best_clusters}")
        print(f"Silhouette Score: {best_score}")

        return best_clusters, cluster_labels, n_clusters_history

    # Criar conexão
    con = duckdb.connect(database="../db/trading_properties_db.duckdb")

    # Carregar registro chaves na mão
    df_chaves_na_mao = con.execute("""
        SELECT 
            * 
        FROM trading_properties_db.chaves_na_mao_schema.chaves_na_mao_register
    """).fetch_df()
    df_chaves_na_mao["fonte_dado"] = "Chaves na Mão"

    # Carregar registro zap imoveis
    df_zap_imoveis = con.execute("""
        SELECT 
            * 
        FROM trading_properties_db.zap_imoveis_schema.zap_imoveis_register
    """).fetch_df()
    df_zap_imoveis["fonte_dado"] = "Zap Imóveis"

    # Carregar registro viva real
    df_viva_real = con.execute("""
        SELECT 
            * 
        FROM trading_properties_db.viva_real_schema.viva_real_register
    """).fetch_df()
    df_viva_real["fonte_dado"] = "Viva Real"

    # Fechar conexão
    con.close()
    
    # Contenar dataframe
    df = pd.concat([df_chaves_na_mao, df_zap_imoveis, df_viva_real], ignore_index=True)

    # Dropar linhas com ids de imóveis iguais
    df = df.sort_values("datahora").drop_duplicates("id", keep="last")

    # Fazer um id com a latitude e longitude específica
    df["lat_long_id"] = df["latitude"].apply(str) + "-" + df["longitude"].apply(str)

    # Contar o numero de acontecimentos daquela latitude e longitude
    df["count_lat_log_id"] = df.groupby("lat_long_id")["id"].transform("count")

    # Transformar latitudes e longitudes repetidas em nulas
    df.loc[df["count_lat_log_id"] > 1, "latitude"] = None
    df.loc[df["count_lat_log_id"] > 1, "longitude"] = None
    
    # Retirar linhas de preco ou tamanho nulo
    df = df.loc[ ( ~ df["tamanho"].isna()) & ( ~ df["preco"].isna()) ]

    # Retirar outliers de preço ou tamanho execivos (3 Z-scores)
    df = df[(np.abs(stats.zscore(df[["tamanho", "preco"]])) < 3).all(axis=1)]

    # Rodar um processo de clusterização GERAL
    _, cluster_labels, _ = find_best_n_of_clusters(df, ["tamanho", "n_banheiros", "n_quartos", "n_garagem"], range(2, 100))

    # Definir coluna de clusterização geral
    df["cluster_geral"] = cluster_labels

    # Instanciar lista para salver dataframes de bairro
    bairro_dataframes = []

    # Rodar um processo de clusterização por bairro
    for bairro in df["bairro"].dropna().unique():
        # Filtrar apenas bairro da iteração
        df_bairro = df.loc[df["bairro"] == bairro].copy()

        # Rodar clusterização
        _, cluster_labels, _= find_best_n_of_clusters(df_bairro, ["tamanho", "n_banheiros", "n_quartos", "n_garagem"], range(2, 30))

        # Definir cluster do bairro
        df_bairro["cluster_bairro"] = cluster_labels

        # Guardar dataframe de bairro
        bairro_dataframes.append(df_bairro)

    # Converter para dataframe
    bairro_dataframe = pd.concat(bairro_dataframes, ignore_index=True)

    # Juntar cluster de bairros ao df original
    df = df.merge(bairro_dataframe[["id", "cluster_bairro"]], on="id", how="left")

    # Marcar cluster de bairro "não encontrado" como nulo
    df.loc[df["bairro"] == "não encontrado", "cluster_bairro"] = None

    # Fazer de preco medio cluster geral e por cluster bairro
    df["preco_medio_cluster_geral"] = df.groupby("cluster_geral")["preco"].transform("mean")
    df["preco_medio_cluster_bairro"] = df.groupby(["bairro", "cluster_bairro"])["preco"].transform("mean")

    # Calcular dif % preço frente a media do cluster
    df["dif_pct_cluster_geral"] = (df["preco"] - df["preco_medio_cluster_geral"]) / df["preco_medio_cluster_geral"]
    df["dif_pct_cluster_bairro"] = (df["preco"] - df["preco_medio_cluster_bairro"]) / df["preco_medio_cluster_bairro"]

    # Calcular o desvio padrão intra cluster geral e intra cluster bairro
    df["std_cluster_geral"] = df.groupby("cluster_geral")["preco"].transform("std")
    df["std_cluster_bairro"] = df.groupby(["bairro", "cluster_bairro"])["preco"].transform("std")

    # Calcular o "Desconto" estatístico
    df["z_value_cluster_geral"] = (df["preco"] - df["preco_medio_cluster_geral"]) / df["std_cluster_geral"] 
    df["z_value_cluster_bairro"] = (df["preco"] - df["preco_medio_cluster_bairro"]) / df["std_cluster_bairro"] 

    # Excluir timezone da colunad e data hotra
    df["datahora"] = df["datahora"].dt.tz_localize(None)

    # Separar apenas colunas necessárias
    df = df[[
        "id", "datahora", "preco", "tamanho", "n_quartos", "n_banheiros",
        "n_garagem", "rua", "bairro", "cidade", 
        "latitude", "longitude", "cluster_geral", 
        "cluster_bairro", "preco_medio_cluster_geral",
        "preco_medio_cluster_bairro", "dif_pct_cluster_geral",
        "dif_pct_cluster_bairro", "z_value_cluster_geral", 
        "z_value_cluster_bairro",
        "fonte_dado", 
        ]]

    # Renomear colunas do entregável
    df.columns = [
        "ID Imóvel", "DataHora", "Preço", "Tamanho (m²)", 
        "Qtd Quartos (#)", "Qtd Banheiros (#)", "Qtd Vagas Garagem (#)",
        "Rua", "Bairro", "Cidade",  "Latitude", "Longitude", 
        "Cluster Geral", "Cluster Bairro",
        "Preço médio do Imóvel (Cluster Geral)", 
        "Preço médio do Imóvel (Cluster Bairro)",
        "Diferença % preço imóvel (Média Cluster Geral)",
        "Diferença % preço imóvel (Média Cluster Bairro)",
        "Z-Valor preço imóvel (Cluster Geral)",
        "Z-Valor preço imóvel (Cluster Bairro)",
        "Site Anúncio"
        ]
    
    # Passar .title no bairro e cidade
    df["Bairro"] = df["Bairro"].str.title()
    df["Cidade"] = df["Cidade"].str.title()

    # Fazer dataframe de descrição dos clusteres
    descritivo_clusters_bairro = df[["Bairro", "Cluster Bairro", "Preço", "Tamanho (m²)", "Qtd Quartos (#)", "Qtd Banheiros (#)", "Qtd Vagas Garagem (#)"]]
    descritivo_clusters = df[["Cluster Geral", "Preço", "Tamanho (m²)", "Qtd Quartos (#)", "Qtd Banheiros (#)", "Qtd Vagas Garagem (#)"]]

    # Agrupar os dataframe por média
    descritivo_clusters_bairro = descritivo_clusters_bairro.groupby(["Bairro", "Cluster Bairro"]).mean()
    descritivo_clusters = descritivo_clusters.groupby("Cluster Geral").mean()

    # Renomear colunas
    descritivo_clusters_bairro = descritivo_clusters_bairro.rename(columns={c: "Média " + c for c in descritivo_clusters_bairro.columns})
    descritivo_clusters = descritivo_clusters.rename(columns={c: "Média " + c for c in descritivo_clusters.columns})

    # Arredondar casas numéricas
    descritivo_clusters_bairro = descritivo_clusters_bairro.round(2)
    descritivo_clusters = descritivo_clusters.round(2)

    # Ordenar por tamanho
    descritivo_clusters_bairro = descritivo_clusters_bairro.sort_values(["Bairro", "Média Tamanho (m²)"])
    descritivo_clusters = descritivo_clusters.sort_values("Média Tamanho (m²)")

    # Resetar index
    descritivo_clusters_bairro = descritivo_clusters_bairro.reset_index()
    descritivo_clusters = descritivo_clusters.reset_index()

    # Definir estilo de borda médio
    medium_border_style = Border(
        left=Side(style="medium"), 
        right=Side(style="medium"), 
        top=Side(style="medium"), 
        bottom=Side(style="medium")
    )

    # Definir estilo de borda thin
    thin_border_style = Border(
        left=Side(style="dashed"), 
        right=Side(style="dashed"), 
        top=Side(style="dashed"), 
        bottom=Side(style="dashed")
    )

    # Fazer woorkbook
    workbook = openpyxl.Workbook()

    # Fazer sheets para armazenar os dados
    worksheet_clusterizacao = workbook.create_sheet(f"Clusterizacao Imoveis")
    worksheet_descritivo_cluster_bairro = workbook.create_sheet(f"Médias dos clusteres de bairro")
    worksheet_descritivo_cluster_geral = workbook.create_sheet(f"Médias os clusteres gerais")

    # Definir estilo do heading
    default_heading_style = openpyxl.styles.NamedStyle(name="default_heading_style")
    default_heading_style.font = Font(bold=True, color="000000")
    default_heading_style.fill = PatternFill(start_color="B0D0FF", end_color="B0D0FF", fill_type="solid")

    # Definir estilo das ceualr de valor
    default_style = openpyxl.styles.NamedStyle(name="value_style")
    default_style.fill = PatternFill(start_color="FEFEFE", end_color="FEFEFE", fill_type="solid")

    # Fazer copia dos dados dos dataframes para evitar problemas de memória
    worksheet_clusterizacao_data = df.copy()
    worksheet_descritivo_cluster_bairro_data = descritivo_clusters_bairro.copy()
    worksheet_descritivo_cluster_geral_data = descritivo_clusters.copy()

    # Carregar as linhas do dataframe pandas
    worksheet_clusterizacao_rows = dataframe_to_rows(worksheet_clusterizacao_data, index=False, header=True)
    worksheet_descritivo_cluster_bairro_rows = dataframe_to_rows(worksheet_descritivo_cluster_bairro_data, index=False, header=True)
    worksheet_descritivo_cluster_geral_rows = dataframe_to_rows(worksheet_descritivo_cluster_geral_data, index=False, header=True)

    # Iterar uma tupla descompactando: linhas, sheet, formatos dos dados
    for rows, worksheet, list_of_formats in [
        (worksheet_clusterizacao_rows, worksheet_clusterizacao, [
            'General', 'mm-dd-yy', 'R$   #,##0.00', '0', '0', '0', 
            '0', 'General', 'General', '0.00', '0.00', 
            'General', '0', '0','R$   #,##0.00', 
            'R$   #,##0.00', '0.00%', '0.00%', '0.00', '0.00', 'General'
        ]), 

        (worksheet_descritivo_cluster_bairro_rows, worksheet_descritivo_cluster_bairro, [
            'General', '0', 'R$   #,##0.00', '0.00', '0.00', '0.00', '0.00',
        ]), 

        (worksheet_descritivo_cluster_geral_rows, worksheet_descritivo_cluster_geral, [
            '0', 'R$   #,##0.00', '0.00', '0.00', '0.00', '0.00',
        ])
        ]:
        # Iterar todas as linhas da worksheet atual e colocar os valores
        for r_idx, row in enumerate(rows, 1):
            for c_idx, value in enumerate(row, 1):
                worksheet.cell(row=r_idx, column=c_idx, value=value)

        # Mudar o estilo do heading
        for row in worksheet.iter_rows(min_row=0, max_row=1):
            # Em cada célula do geading
            for cell in row:
                # Mudar o estilo para estilo heading
                cell.style = default_heading_style

                # Mudar a borda
                cell.border = medium_border_style

        # Mudar o estilo das linhas
        for r_idx, row in enumerate(worksheet.iter_rows(min_row=2)):
            # Em cada célula
            for cell_idx, cell in enumerate(row):
                # Colcoar estilo default
                cell.style = default_style

                # Mudar a borda
                cell.border = thin_border_style

                # Mudar formatação
                cell.number_format = list_of_formats[cell_idx]

    # Dropar sheet padrão
    del workbook["Sheet"]

    # Salvar a versão formatada
    workbook.save(f"../analytics/Clusterizacao Imoveis ({datetime.today().strftime('%Y_%m_%d')}).xlsx")

    # Sem retorno
    return None

# Rodar como script
if __name__ == '__main__':
    main()
