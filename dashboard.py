import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Transacciones en Tiempo Real",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración de la base de datos
db_config = {
    "user": "powerbi",
    "password": "ytmfxN8DUsfm4",
    "host": "10.41.235.23",
    "port": "5432",
    "dbname": "productiondb"
}

# Crear conexión con PostgreSQL
@st.cache_resource
def create_db_connection():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        return engine
    except Exception as e:
        st.error(f"Error al conectar con la base de datos: {e}")
        return None

# Consulta SQL con filtro de fecha dinámico
def get_query(hours_back=24):
    return f"""
    select 
        case 
        WHEN t.idgatewayrule IN(1,2,645) THEN 'Agregador' 
        ELSE 'Gateway' 
        END AS tipopasarela,
        c."name" AS comercio,
        s."name" AS servicio, 
        DATE(t2.transdate) AS fecha,
        t3.description AS status,
        g2."name" AS pasarela,
        coalesce(errmsgpgw,t.responsepgw) AS respuesta,
        a.cardtype AS tipotarjeta,
        a.franchise AS franquicia,
        a.issuingbank,    	
        COUNT(t.idtransaction) AS cantidad_transacciones, 
        SUM(t2.billvalue) AS valor
    FROM trns.transretries t
    inner join trns.transactions t2 on t2.idtransaction = t.idtransaction 
    inner join trns.transactionstatus t3 on t3.idstatus = t2.status 
    inner join assc.services s on s.idservice = t2.idservice 
    inner join assc.commerces c on t2.idcommerce = c.idcommerce
    inner join pycl.gatewayrules g on g.idgatewayrule = t.idgatewayrule  
    inner join pycl.gateway g2 on g2.idgateway = g.idgateway 
    inner join gpac.acccards a on a.idcard = t.idcard 
    WHERE  coalesce (errmsgpgw,'') NOT IN('INVALID_REQUEST','type card not allowed')
    AND t2.transdate >= NOW() - INTERVAL '{hours_back} hours'
    group by 
        case when t.idgatewayrule in(1,2,645) then 'Agregador' else 'Gateway' end,
        c."name",
        s."name",
        DATE(t2.transdate),
        t3.description,
        g2."name",
        coalesce(errmsgpgw,t.responsepgw),
        a.cardtype,
        a.franchise, 
        a.issuingbank;
    """

# Obtener datos de la base de datos
@st.cache_data(ttl=300)  # Cache por 5 minutos
def get_data(hours_back=24):
    engine = create_db_connection()
    if engine:
        try:
            query = get_query(hours_back)
            df = pd.read_sql_query(query, engine)
            return df
        except Exception as e:
            st.error(f"Error al ejecutar la consulta: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# Sidebar para controles
with st.sidebar:
    st.title("⚙️ Controles del Dashboard")
    
    horas_atras = st.slider(
        "Período de análisis (horas atrás):",
        min_value=1,
        max_value=72,
        value=24
    )
    
    intervalo_actualizacion = st.slider(
        "Intervalo de actualización (segundos):",
        min_value=30,
        max_value=300,
        value=60
    )
    
    auto_update = st.checkbox("Actualización automática", value=True)
    
    if st.button("Actualizar ahora"):
        st.cache_data.clear()

# Título principal
st.title("📊 Dashboard de Transacciones en Tiempo Real")
st.caption(f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Obtener datos
df = get_data(horas_atras)

if df.empty:
    st.warning("No se encontraron datos para el período seleccionado.")
else:
    # Mostrar KPIs principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_transacciones = df['cantidad_transacciones'].sum()
        st.metric("Total Transacciones", f"{total_transacciones:,}")
    
    with col2:
        total_valor = df['valor'].sum()
        st.metric("Valor Total", f"${total_valor:,.2f}")
    
    with col3:
        transacciones_exitosas = df[df['status'] == 'APPROVED']['cantidad_transacciones'].sum()
        tasa_exito = (transacciones_exitosas / total_transacciones * 100) if total_transacciones > 0 else 0
        st.metric("Tasa de Éxito", f"{tasa_exito:.2f}%")
    
    with col4:
        avg_ticket = total_valor / total_transacciones if total_transacciones > 0 else 0
        st.metric("Ticket Promedio", f"${avg_ticket:.2f}")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        # Transacciones por tipo de pasarela
        transacciones_pasarela = df.groupby('tipopasarela')['cantidad_transacciones'].sum().reset_index()
        fig1 = px.pie(transacciones_pasarela, values='cantidad_transacciones', names='tipopasarela', 
                     title='Distribución por Tipo de Pasarela')
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Transacciones por estado
        transacciones_status = df.groupby('status')['cantidad_transacciones'].sum().reset_index()
        fig2 = px.bar(transacciones_status, x='status', y='cantidad_transacciones', 
                     title='Transacciones por Estado')
        st.plotly_chart(fig2, use_container_width=True)
    
    # Transacciones por franquicia
    transacciones_franquicia = df.groupby('franquicia')['cantidad_transacciones'].sum().reset_index()
    fig3 = px.bar(transacciones_franquicia, x='franquicia', y='cantidad_transacciones', 
                 title='Transacciones por Franquicia')
    st.plotly_chart(fig3, use_container_width=True)
    
    # Evolución temporal
    df['fecha'] = pd.to_datetime(df['fecha'])
    evolucion = df.groupby('fecha').agg({
        'cantidad_transacciones': 'sum',
        'valor': 'sum'
    }).reset_index()
    
    fig4 = px.line(evolucion, x='fecha', y='cantidad_transacciones', 
                  title='Evolución de Transacciones en el Tiempo')
    st.plotly_chart(fig4, use_container_width=True)
    
    # Mostrar datos tabulares
    with st.expander("Ver datos detallados"):
        st.dataframe(df)

# Actualización automática
if auto_update:
    time.sleep(intervalo_actualizacion)
    st.rerun()
