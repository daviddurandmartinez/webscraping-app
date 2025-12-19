import streamlit as st
from PIL import Image
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from config import FECHA_ACTUAL,FECHA_ANTERIOR,FECHA_HORA,HORA_ACTUAL,URL_RPP,URL_GESTION,URL_CLIMA,PALABRAS_CLAVE,KEY_COLUMN
from database_connector import create_sqlalchemy_engine,run_upsert_process
import io

def main():

    ###############
    ## STREAMLIT ##
    ###############

    #Imagenes en caché para realizar una sola subida    
    @st.cache_resource
    def load_image(image_path):
        return Image.open(image_path)
    ruta_logo = "assets/logo_newport.png"
    img = load_image(ruta_logo)

    st.set_page_config(
        page_title="Noticias y Clima", 
        page_icon=img,
        layout="wide", # Usamos wide para más espacio
        initial_sidebar_state="collapsed"
    )

    st.title("Ocurrencias")
    st.header("Información en vivo de Noticias y clima")
    st.markdown("Fuente noticias: RPP - GESTION | Fuente clima: weather.com")
    
    #################################################################
    ## OBTENER DATAFRAME DE METODO SCRAPING NOTICIAS Y TEMPERATURA ##
    #################################################################

    with st.spinner('Extrayendo información en vivo...'):
        df = get_gestion_data() # Ahora retorna el DF limpio

    if not df.empty:
        # Intentar Guardar en SQL y mostrar estado
        engine = create_sqlalchemy_engine()
        if engine:
            success, message = run_upsert_process(df, engine)
            if success:
                st.success(message)
            else:
                st.error(message)

    #####################
    ## MOSTRAR FILTROS ##
    #####################

    st.sidebar.header("Por favor, filtre aquí:")
    fuente = st.sidebar.multiselect(
        "Seleccione la Fuente:",
        options=df["fuente"].unique(),
        default=df["fuente"].unique(),
    )
    tipo = st.sidebar.multiselect(
        "Seleccione el tipo:",
        options=df["tipo"].unique(),
        default=df["tipo"].unique(),
    )
    distrito = st.sidebar.multiselect(
        "Seleccione el tipo:",
        options=df["distrito"].unique(),
        default=df["distrito"].unique(),
    )
    df_seleccion = df.query("fuente == @fuente & tipo ==@tipo & distrito ==@distrito")

    ########################
    ## MOSTRAR DATAFRAMES ##
    ########################

    st.dataframe(df_seleccion, width="stretch")

    ########################################
    ## DESCARGA DE DATOS EN ARCHIVO EXCEL ##
    ########################################

    # Convertir DataFrame a Excel en memoria (mejor práctica para Streamlit)
    excel_buffer = io.BytesIO()
    df_seleccion.to_excel(excel_buffer, index=False, engine='xlsxwriter')
    excel_buffer.seek(0)
    st.download_button(
        label="Descargar",
        data=excel_buffer,
        file_name=f"export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
     )

def get_conecta_sql_server(df: pd.DataFrame):
    # Inicialización del motor una sola vez 
    engine = create_sqlalchemy_engine() 
    if engine is None:
        # Mensaje de error ajustado
        st.warning("Verifica que los drivers de ODBC estén instalados y las credenciales sean correctas")
        return 
    
    try:
        # 1. Separar la cadena KEY_COLUMN en una lista de nombres de columnas
        columnas_clave = [col.strip() for col in KEY_COLUMN.split(',')]

        # 2. Verificar que TODAS las columnas clave existan en el DataFrame
        columnas_faltantes = [col for col in columnas_clave if col not in df.columns]

        if columnas_faltantes:
            # Reportar solo las columnas que faltan
            st.error(
                f"Error de archivo: La columna(s) de ID '{', '.join(columnas_faltantes)}' "
                f"definida(s) en 'config.py' no se encontró(eron) en el DataFrame."
            )
            return
            
        run_upsert_process(df, engine)              
    except Exception as e:
        # Captura errores inesperados al procesar.
        st.error(f"Error inesperado al procesar el archivo o la lógica: {e}")

def get_fecha_hora(hora):
    hora_web = datetime.strptime(hora[0:5],'%H:%M').time()
    hora_actual = datetime.now().time()
    if hora_actual >= hora_web:
        return FECHA_ACTUAL
    else:
        return FECHA_ANTERIOR
    
def get_gestion_content(url):
    response = requests.get(url) #Solicitud http y obtener contenido
    if response.status_code == 200: #Si la respuesta es exitosa
        content = response.text #Obtenemos contenido del maquetadao
        soup = BeautifulSoup(content,'html.parser') #Pasar el contenido HTML a objeto soup
        return soup
    else:
        return None
    
@st.cache_data(ttl=600) # Caché por 10 min para no saturar las webs
def get_gestion_data():
    LISTA_GESTION_CLIMA = []

    # #FUENTE: RPP (NOTICIAS)
    for categoria, url in URL_RPP.items():
        soup = get_gestion_content(url)
        noticias = soup.find_all('div', class_='news__data') 
        for noticia in noticias:
            try:
                titulo = noticia.find('h2', class_='news__title').find('a').get_text(strip=True)
                hora = noticia.find('div', class_='news__info').find('time').get_text(strip=True)
                hora_noticia = datetime.strptime(hora[0:5],'%H:%M').strftime('%H:%M')
                fecha_noticia = get_fecha_hora(hora)
            except AttributeError:
                titulo = None 
                
            if titulo != None:
                    LISTA_GESTION_CLIMA.append({
                         'fecha': fecha_noticia,
                         'hora': hora_noticia,
                         'noticia': titulo,
                         'temperatura': None,
                         'distrito': None,
                         'tipo':categoria,
                         'fuente':'rpp',
                         'actualizacion': FECHA_HORA
                         })         

    #FUENTE: GESTION (NOTICIAS)
    for categoria, url in URL_GESTION.items():
        soup = get_gestion_content(url)
        noticias = soup.find_all('div', class_='story-item__bottom flex lg:pb-15')
        for noticia in noticias:
            try:
                titulo = noticia.find('div', class_='story-item__information-box w-full').find('a').get_text(strip=True)
                fecha = noticia.find('div', class_='story-item__top flex items-center md:flex-col md:items-start').find('p').get_text(strip=True)
                hora_noticia = datetime.strptime(fecha[-5:],'%H:%M').strftime('%H:%M')
                fecha_noticia = datetime.strptime(fecha[:10],'%d/%m/%Y').date()
            except ValueError:
                titulo = None
                hora_noticia = HORA_ACTUAL
                fecha_noticia = FECHA_ACTUAL

            if titulo != None:
                LISTA_GESTION_CLIMA.append({
                            'fecha': fecha_noticia,
                            'hora': hora_noticia,
                            'noticia': titulo,
                            'temperatura': None,
                            'distrito': None,
                            'tipo': categoria,
                            'fuente':'gestion',
                            'actualizacion': FECHA_HORA
                            })         
        #df = pd.DataFrame(LISTA_GESTION)

    #FUENTE: CLIMA
    for categoria, url in URL_CLIMA.items():
        soup = get_gestion_content(url) 
        temperaturas = soup.find_all('a', class_='Column--innerWrapper--kyyeB Column--verticalStack--k9S2a Button--default--osTe5')   
        sublista_gestion = []     
        for item in temperaturas:
            try:
                hora = item.find('h3', class_='Column--label--tMb5q Column--small--oEVgP Column--verticalStack--k9S2a').find('span').get_text(strip=True)
                temperatura = item.find('div', class_='Column--temp--XitCX columnTempHiWrapper Column--verticalStack--k9S2a').find('span').get_text(strip=True)
                if hora == 'Ahora':
                    hora_temperatura = datetime.strptime(HORA_ACTUAL,'%H:%M').strftime('%H:%M')
                else:
                    hora_temperatura = hora
            except AttributeError:
                None
                hora_temperatura = None
                temperatura = None

            if temperatura != None:
                sublista_gestion.append({
                            'fecha': FECHA_ACTUAL,
                            'hora_web': hora,
                            'hora': hora_temperatura,
                            'noticia': 'Clima en '+categoria,
                            'temperatura': temperatura,
                            'distrito': categoria,
                            'tipo':'clima',
                            'fuente':'weather',
                            'actualizacion': FECHA_HORA
                            })

        for i, dic in enumerate(sublista_gestion):
           if dic['hora_web'] == 'Ahora':
               indice = i
               break
        LISTA_GESTION_CLIMA.extend(sublista_gestion[indice:indice+5])

        df = pd.DataFrame(LISTA_GESTION_CLIMA,columns=['fecha','hora','noticia','temperatura','distrito','tipo','fuente','actualizacion'])

    #busqueda = '|'.join(PALABRAS_CLAVE)
    #df_filtrado = df[df['noticia'].str.contains(busqueda, case=False, na=False)]

    return df

if __name__ == '__main__':
    main()