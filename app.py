import streamlit as st
import polars as pl
import pandas as pd
import numpy as np
import gc

# ==============================================================================
# 1. CONFIGURAÇÃO E UTILITÁRIOS
# ==============================================================================

pd.set_option("styler.render.max_elements", 1000000)

def setup_page():
    """Configurações iniciais da página e CSS."""
    st.set_page_config(
        page_title="Comparador de Planilhas",
        page_icon="images/logo-obs.png",
        layout="wide"
    )

    st.sidebar.image(
        "images/OBS_Logo.png", 
        width=250
    )
    
    st.markdown("""
        <style>
        .block-container {padding-top: 2rem; padding-bottom: 3rem;}
        div[data-testid="stMetricValue"] {font-size: 1.4rem;}
        section[data-testid="stSidebar"] hr {margin: 1rem 0;}
        div[data-testid="stExpander"] {background-color: #f8f9fa; border-radius: 8px;}
        </style>
    """, unsafe_allow_html=True)

@st.cache_data(show_spinner=False, ttl="2h")
def load_data(uploaded_file):
    """Carrega o arquivo para Polars DataFrame."""
    if uploaded_file is None:
        return None
    
    filename = uploaded_file.name.lower()
    try:
        if filename.endswith('.csv'):
            try:
                return pl.read_csv(uploaded_file.getvalue(), infer_schema_length=10000)
            except:
                return pl.read_csv(uploaded_file.getvalue(), separator=';', infer_schema_length=10000)
        elif filename.endswith(('.xlsx', '.xls')):
            return pl.read_excel(uploaded_file)
    except Exception as e:
        st.sidebar.error(f"Erro ao ler {uploaded_file.name}: {e}")
        return None

# ==============================================================================
# 2. LÓGICA DE PROCESSAMENTO
# ==============================================================================

def normalize_types(df1: pl.DataFrame, df2: pl.DataFrame):
    """Garante compatibilidade de tipos."""
    cols_comuns = set(df1.columns).intersection(set(df2.columns))
    casts_df1, casts_df2 = [], []

    for col in cols_comuns:
        # Pula as colunas de linha que acabamos de criar
        if col in ["Linha_Original_A", "Linha_Original_B"]: continue

        dtype1, dtype2 = df1[col].dtype, df2[col].dtype
        if dtype1 != dtype2:
            casts_df1.append(pl.col(col).cast(pl.String).str.strip_chars())
            casts_df2.append(pl.col(col).cast(pl.String).str.strip_chars())
        elif dtype1 == pl.String:
            casts_df1.append(pl.col(col).str.strip_chars())
            casts_df2.append(pl.col(col).str.strip_chars())

    if casts_df1: df1 = df1.with_columns(casts_df1)
    if casts_df2: df2 = df2.with_columns(casts_df2)
    return df1, df2

def execute_comparison(df1: pl.DataFrame, df2: pl.DataFrame, sort_keys: list):
    
    # 1. CRIAÇÃO DO ÍNDICE ORIGINAL (ANTES DE ORDENAR)
    # Offset=2 considera que Excel tem cabeçalho na linha 1, dados começam na 2.
    df1 = df1.with_row_index(name="Linha_Original_A", offset=2)
    df2 = df2.with_row_index(name="Linha_Original_B", offset=2)

    # 2. Validação de Estrutura (ignorando as colunas de linha criadas agora)
    cols1 = set(c for c in df1.columns if c != "Linha_Original_A")
    cols2 = set(c for c in df2.columns if c != "Linha_Original_B")
    
    if cols1 != cols2:
        return {"status": "error", "msg": "Colunas diferentes.", "details": f"Diff: {cols1.symmetric_difference(cols2)}"}

    # 3. Normalização
    df1, df2 = normalize_types(df1, df2)

    # 4. ORDENAÇÃO (Lógica Solicitada)
    # Remove as colunas de linha da lista de ordenação
    cols_para_ordenar = [c for c in df1.columns if c not in ["Linha_Original_A"]]
    
    try:
        if sort_keys:
            # Opção A: Usuário definiu chaves -> Ordena por elas (Mais seguro para updates)
            df1 = df1.sort(sort_keys, maintain_order=True)
            df2 = df2.sort(sort_keys, maintain_order=True)
        else:
            # Opção B: Usuário NÃO definiu chaves -> Ordena por TUDO (Conteúdo)
            # Isso garante que linhas idênticas fiquem alinhadas mesmo se estiverem em ordem física diferente
            df1 = df1.sort(cols_para_ordenar, maintain_order=True)
            df2 = df2.sort(cols_para_ordenar, maintain_order=True)
            
    except Exception as e:
        return {"status": "error", "msg": f"Erro na ordenação: {str(e)}"}

    if df1.height != df2.height:
        st.toast(f"Tamanhos diferentes ({df1.height} vs {df2.height})", icon="⚠️")
        return {
            "status": "size_error",
            "msg": f"As tabelas possuem tamanhos diferentes ({df1.height} vs {df2.height}) e não podem ser comparadas linha a linha.",
            "details": "Verifique se há linhas duplicadas ou faltantes em um dos arquivos."
        }

    # 5. Detecção
    diff_results = {}
    cols_with_diff = []
    global_error_mask = None
    
    check_cols = [c for c in df1.columns if c not in ["Linha_Original_A"]]
    
    prog_bar = st.progress(0, text="Comparando...")
    total = len(check_cols)

    for i, col in enumerate(check_cols):
        prog_bar.progress((i + 1) / total)
        
        # Compara (ignora colunas de linha)
        mask = df1[col].ne_missing(df2[col])
        count = mask.sum()

        if global_error_mask is None: global_error_mask = mask
        else: global_error_mask = global_error_mask | mask

        if count > 0:
            # Seleciona: Linha A | Linha B | Valor A | Valor B
            v1 = df1.filter(mask).select([pl.col("Linha_Original_A"), pl.col(col).alias("Valor_Arq_A")])
            v2 = df2.filter(mask).select([pl.col("Linha_Original_B"), pl.col(col).alias("Valor_Arq_B")])
            
            combined = pl.concat([v1, v2], how="horizontal")
            
            # Reorganiza colunas para visualização
            combined = combined.select(["Linha_Original_A", "Linha_Original_B", "Valor_Arq_A", "Valor_Arq_B"])
            
            diff_results[col] = {"count": count, "data": combined}
            cols_with_diff.append(col)

    prog_bar.empty()
    
    # Prepara linhas completas
    total_rows_with_error = 0
    full_rows_a, full_rows_b = None, None

    if global_error_mask is not None and global_error_mask.sum() > 0:
        total_rows_with_error = global_error_mask.sum()
        full_rows_a = df1.filter(global_error_mask)
        full_rows_b = df2.filter(global_error_mask)
    
    return {
        "status": "success", "diffs": diff_results, "cols_diff": cols_with_diff,
        "total_rows": df1.height, "rows_with_error": total_rows_with_error,
        "full_rows_a": full_rows_a, "full_rows_b": full_rows_b,
        "sorted_by": sort_keys if sort_keys else "Todas as colunas (Automático)"
    }

# ==============================================================================
# 3. INTERFACE DA SIDEBAR
# ==============================================================================

def render_sidebar_interface():
    with st.sidebar:
        st.header("📂 Arquivos")
        f1 = st.file_uploader("Arquivo A (Referência)", type=["xlsx", "csv"])
        f2 = st.file_uploader("Arquivo B (Teste)", type=["xlsx", "csv"])
        
        df_a, df_b, name_a, name_b, sort_keys, run_btn = None, None, None, None, [], False

        if f1 and f2:
            name_a, name_b = f1.name, f2.name
            with st.spinner("Lendo..."):
                df_a, df_b = load_data(f1), load_data(f2)

            if df_a is not None and df_b is not None:
                st.divider()
                st.header("⚙️ Configuração")
                
                # Remove colunas de linha se já existirem no arquivo (caso raro)
                cols = sorted(list(set(df_a.columns).intersection(set(df_b.columns))))

                sort_keys = st.multiselect(
                    "Chave de ordenação (ID):", 
                    options=cols,
                    placeholder="Padrão: Todas as colunas",
                    help="Selecione as colunas que identificam a linha (ex: ID, UF+Setor)."
                )
                
                st.write(""); run_btn = st.button("Comparar planilhas", type="primary", width='stretch')
                if st.button("Limpar cache"): st.cache_data.clear(); gc.collect()
        else: st.info("Faça o upload dos arquivos.")
    return df_a, name_a, df_b, name_b, sort_keys, run_btn

# ==============================================================================
# 4. LÓGICA DE ESTILO E RENDERIZAÇÃO
# ==============================================================================

def highlight_differences(df_to_style, df_comparison, common_columns):
    """
    Função vetorizada para aplicar estilos.
    Retorna um objeto Styler do Pandas.
    """
    def style_logic(data):
        # 'data' aqui é o DataFrame inteiro sendo estilizado (df_to_style)
        # Criamos um DataFrame vazio de strings para os estilos
        styles = pd.DataFrame('', index=data.index, columns=data.columns)
        
        # Converte para numpy para performance
        # Alinha os dados apenas nas colunas comuns (dados de negócio)
        # Assumimos que os DataFrames estão alinhados pelo índice (resetado antes)
        
        # Pega valores das colunas comuns
        val1 = data[common_columns].fillna("$$NULL$$").astype(str).values
        val2 = df_comparison[common_columns].fillna("$$NULL$$").astype(str).values
        
        # Máscara booleana onde difere
        mask = val1 != val2
        
        # Cor de fundo para erros (vermelho claro)
        bg_color = 'background-color: #ffcccc; color: #9a0000; font-weight: bold;'
        
        # Aplica o estilo no DataFrame de estilos (apenas nas colunas comuns)
        for idx_col, col_name in enumerate(common_columns):
            # idx_col mapeia para a coluna no array numpy 'mask'
            # Mas precisamos mapear de volta para o DataFrame de styles
            styles.loc[mask[:, idx_col], col_name] = bg_color
            
        return styles

    return df_to_style.style.apply(style_logic, axis=None)

def render_previews(df_a, name_a, df_b, name_b):
    """Exibe amostra dos dados."""
    st.subheader("Prévia dos arquivos")
    c1, c2 = st.columns(2)
    with c1:
        st.dataframe(df_a.head(5), width='stretch')
        st.caption(f"{df_a.height} linhas x {df_a.width} colunas")
    with c2:
        st.dataframe(df_b.head(5), width='stretch')
        st.caption(f"{df_b.height} linhas x {df_b.width} colunas")

def render_diff_table(col_name, diff_data, name_a, name_b):
    """
    Helper para renderizar a tabela de diferenças lado a lado.
    Recebe o DataFrame combinado e separa visualmente.
    """
    st.caption(f"Exibindo divergências na coluna: **{col_name}** ({len(diff_data)} registros)")
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.markdown(f"**📄 {name_a}**")
        # Seleciona apenas as colunas do Arq A
        df_show_a = diff_data.select([
            pl.col("Linha_Original_A").alias("Linha"), 
            pl.col("Valor_Arq_A").alias(f"Valor em {col_name}")
        ])
        st.dataframe(df_show_a)

    with col_b:
        st.markdown(f"**📄 {name_b}**")
        # Seleciona apenas as colunas do Arq B
        df_show_b = diff_data.select([
            pl.col("Linha_Original_B").alias("Linha"), 
            pl.col("Valor_Arq_B").alias(f"Valor em {col_name}")
        ])
        st.dataframe(df_show_b)

def render_results(result, name_a, name_b):
    """Exibe KPIs, Detalhes Lado a Lado e Linhas Completas."""
    if result["status"] == "size_error":
        st.error("🛑 " + result["msg"])
        st.warning(result["details"])
        return

    if result["status"] == "error":
        st.error(result["msg"])
        if "details" in result: st.code(result["details"])
        return

    cols_diff = result["cols_diff"]
    diffs = result["diffs"]
    
    st.divider()
    st.subheader("Relatório de comparação")
    
    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total de linhas", f"{result['total_rows']:,}".replace(",", "."))
    k2.metric("Colunas com divergência", len(cols_diff))
    rows_err = result['rows_with_error']
    k3.metric("Linhas com divergência", f"{rows_err:,}".replace(",", "."))

    with k4:
        if not cols_diff:
            st.success("✅ **AS TABELAS SÃO IGUAIS**")
        else:
            st.error("⚠️ **AS TABELAS SÃO DIVERGENTES**")

    st.divider()
    
    if not cols_diff:
        st.balloons()
        st.success("Tudo certo! Arquivos idênticos.")
        return

    # 1. VISUALIZAÇÃO POR COLUNA (LADO A LADO)
    st.markdown(f"### Diferenças por coluna")
    
    if len(cols_diff) > 8:
        # Se houver muitas colunas, usa selectbox
        selected = st.selectbox("Selecione a coluna para detalhar:", cols_diff)
        d = diffs[selected]
        render_diff_table(selected, d["data"], name_a, name_b)
    else:
        # Se houver poucas, usa abas
        tabs = st.tabs(cols_diff)
        for tab, col in zip(tabs, cols_diff):
            with tab:
                d = diffs[col]
                render_diff_table(col, d["data"], name_a, name_b)

    # 2. VISUALIZAÇÃO DE LINHAS COMPLETAS
    st.divider()
    st.subheader("Linhas completas onde estão as divergências")

    # Converte para Pandas APENAS o dataset filtrado (leve para memória)
    # Reset index para garantir alinhamento perfeito na comparação
    df_a_pd = result["full_rows_a"].to_pandas()
    df_b_pd = result["full_rows_b"].to_pandas()
    
    # Identifica colunas de dados (exclui índices de linha)
    common_cols = [c for c in df_a_pd.columns if c not in ["Linha_Original_A", "Linha_Original_B"]]

    tab_a, tab_b = st.tabs([f"Linhas do Arquivo A ({name_a})", f"Linhas do Arquivo B ({name_b})"])
    
    with tab_a:
        # Estiliza A comparando com B
        styler_a = highlight_differences(df_a_pd, df_b_pd, common_cols)
        # Formata colunas de index para não mostrar vírgula
        styler_a.format({"Linha_Original_A": "{:.0f}"})
        st.dataframe(styler_a)
        
    with tab_b:
        # Estiliza B comparando com A
        styler_b = highlight_differences(df_b_pd, df_a_pd, common_cols)
        styler_b.format({"Linha_Original_B": "{:.0f}"})
        st.dataframe(styler_b)

# ==============================================================================
# 5. MAIN
# ==============================================================================

def main():
    setup_page()
    st.title("Comparador de Planilhas")
    
    df_a, name_a, df_b, name_b, keys, run_btn = render_sidebar_interface()
    
    if df_a is not None and df_b is not None:
        render_previews(df_a, name_a, df_b, name_b)
        
        if run_btn:
            res = execute_comparison(df_a, df_b, keys)
            # Passamos os nomes dos arquivos para render_results para usar nas abas
            render_results(res, name_a, name_b)
            
    elif df_a is None or df_b is None:
        st.markdown("""
        ### Como usar:
        1. Utilize a **barra lateral** à esquerda para carregar seus arquivos.
        2. Selecione uma ou mais colunas chave (ID) para garantir a ordenação correta das tabelas. Mantenha vazio para usar todas as colunas. 
        3. Clique em **Comparar planilhas**.
        """)

if __name__ == "__main__":
    main()