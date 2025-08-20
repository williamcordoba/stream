# dashboard_tickets.py
import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import plotly.express as px

# Configuración de la página
st.set_page_config(
    page_title="Dashboard Tickets NOC",
    page_icon="📊",
    layout="wide"
)

# Conexión con SQLAlchemy (más estable)
@st.cache_resource
def init_connection():
    try:
        connection_string = f"mysql+pymysql://{st.secrets['DB_USER']}:{st.secrets['DB_PASSWORD']}@{st.secrets['DB_HOST']}:{st.secrets.get('DB_PORT', 3306)}/{st.secrets['DB_NAME']}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

# Función para ejecutar tu query
@st.cache_data(ttl=300)
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
    
    engine = init_connection()
    if engine:
        try:
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            st.error(f"Error ejecutando query: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# Resto del código igual...
def create_visualizations(df):
    # (Mismo código de visualización que antes)
    pass

def main():
    # (Mismo código principal que antes)
    pass

if __name__ == "__main__":
    main()
