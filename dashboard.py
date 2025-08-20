import streamlit as st
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import time
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Tickets NOC",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Conexión a la base de datos (usar secrets en producción)
@st.cache_resource
def init_connection():
    try:
        conn = mysql.connector.connect(
            host=st.secrets["DB_HOST"],
            database=st.secrets["DB_NAME"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"],
            port=st.secrets.get("DB_PORT", 3306)
        )
        return conn
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

# Función para ejecutar tu query
@st.cache_data(ttl=300)  # Cache por 5 minutos
def get_tickets_data(fecha_inicio, fecha_fin):
    query = f"""
    SELECT
        tickets.Usuario_Asignado,
        tickets.Ticket_ID AS ID_Tickets,
        DATE(tickets.`Fecha solucion`) AS Fecha_solucion,
        estado_solicitud.`Estado Solicitud`,
        tickets.Grupo
    FROM 
    (
        SELECT 'EN CURSO' AS `Estado Solicitud`
        UNION ALL
        SELECT 'RESUELTO'
        UNION ALL
        SELECT 'CERRADO'
    ) AS estado_solicitud
    LEFT JOIN 
    (
        SELECT 
            gt.id AS Ticket_ID, 
            CASE
                WHEN gt.status = 5 THEN 'RESUELTO'
                WHEN gt.status = 6 THEN 'CERRADO'
            END AS `Estado Solicitud`,
            gt.solvedate AS `Fecha solucion`,
            gt.name,
            ge.name AS Entidad,
            (
             SELECT gg.name 
             FROM glpi_groups_tickets ggt 
             INNER JOIN glpi_groups gg ON gg.id = ggt.groups_id
             WHERE ggt.tickets_id = gt.id 
               AND ggt.type = 2
               AND gg.name = 'NOC'
             LIMIT 1
            ) AS Grupo,
            gu.name AS Usuario_Asignado,
            gl.name AS Comercio,
            gl.completename AS Comercio_completo
        FROM 
            glpi_tickets gt
        INNER JOIN 
            glpi_entities ge ON ge.id = gt.entities_id  
        INNER JOIN 
            glpi_users gu ON gu.id = gt.users_id_recipient 
        LEFT JOIN 
            glpi_locations gl ON gl.id = gt.locations_id 
        WHERE 
            gt.solvedate BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
             AND gt.id IS NOT NULL
             AND gt.is_deleted = 0
    ) AS tickets
    ON 
        estado_solicitud.`Estado Solicitud` = tickets.`Estado Solicitud`
    WHERE tickets.Grupo IS NOT NULL
    GROUP BY 
        estado_solicitud.`Estado Solicitud`,
        tickets.Usuario_Asignado,
        DATE(tickets.`Fecha solucion`),
        tickets.Grupo
    """
    
    conn = init_connection()
    if conn:
        try:
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Error ejecutando query: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# Función para crear visualizaciones
def create_visualizations(df):
    if df.empty:
        st.warning("No hay datos para mostrar")
        return
    
    # KPI principales
    col1, col2, col3, col4 = st.columns(4)
    
    total_tickets = len(df)
    tickets_resueltos = len(df[df['Estado Solicitud'] == 'RESUELTO'])
    tickets_cerrados = len(df[df['Estado Solicitud'] == 'CERRADO'])
    usuarios_activos = df['Usuario_Asignado'].nunique()
    
    with col1:
        st.metric("Total Tickets", total_tickets)
    with col2:
        st.metric("Resueltos", tickets_resueltos)
    with col3:
        st.metric("Cerrados", tickets_cerrados)
    with col4:
        st.metric("Usuarios Activos", usuarios_activos)
    
    # Gráficos
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        # Tickets por estado
        estado_count = df['Estado Solicitud'].value_counts()
        fig1 = px.pie(
            values=estado_count.values,
            names=estado_count.index,
            title="Distribución por Estado"
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col_chart2:
        # Tickets por usuario
        usuario_count = df['Usuario_Asignado'].value_counts().head(10)
        fig2 = px.bar(
            x=usuario_count.values,
            y=usuario_count.index,
            orientation='h',
            title="Top 10 Usuarios por Tickets",
            labels={'x': 'N° Tickets', 'y': 'Usuario'}
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    # Serie temporal
    st.subheader("Evolución Temporal")
    temporal_data = df.groupby(['Fecha_solucion', 'Estado Solicitud']).size().reset_index(name='count')
    fig3 = px.line(
        temporal_data,
        x='Fecha_solucion',
        y='count',
        color='Estado Solicitud',
        title="Tickets por Fecha y Estado"
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    # Dataframe detallado
    st.subheader("Detalle de Tickets")
    st.dataframe(df, use_container_width=True)

# Interfaz principal
def main():
    st.title("📊 Dashboard de Tickets NOC")
    st.markdown("---")
    
    # Selectores de fecha
    col_fecha1, col_fecha2 = st.columns(2)
    with col_fecha1:
        fecha_inicio = st.date_input(
            "Fecha inicio",
            value=datetime(2025, 7, 1),
            min_value=datetime(2020, 1, 1)
        )
    with col_fecha2:
        fecha_fin = st.date_input(
            "Fecha fin",
            value=datetime(2025, 8, 8),
            min_value=datetime(2020, 1, 1)
        )
    
    # Botón para actualizar
    if st.button("🔄 Actualizar Datos", type="primary"):
        st.cache_data.clear()
    
    # Obtener datos
    df = get_tickets_data(fecha_inicio, fecha_fin)
    
    if not df.empty:
        create_visualizations(df)
        
        # Mostrar última actualización
        st.sidebar.info(f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Estadísticas en sidebar
        st.sidebar.subheader("📈 Estadísticas")
        st.sidebar.metric("Total registros", len(df))
        st.sidebar.metric("Período analizado", f"{fecha_inicio} a {fecha_fin}")
        
    else:
        st.warning("No se encontraron datos para el período seleccionado")

if __name__ == "__main__":
    main()
