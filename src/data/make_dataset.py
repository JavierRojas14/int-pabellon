# -*- coding: utf-8 -*-
import glob
import hashlib
import json
import logging
import os
import warnings
from pathlib import Path

import click
import numpy as np
import pandas as pd
from dotenv import find_dotenv, load_dotenv

# Ignora UserWarnings debido a que las bases de datos presentan validacion de datos
warnings.filterwarnings("ignore", category=UserWarning)

COLUMNAS_UTILES = [
    "FECHA ",
    "SEXO SELECCIONAR LISTA",
    "FICHA",
    "EDAD ",
    "PREV: SELECCIONAR LISTA",
    "ESPECIALIDAD  SELECCIONAR LISTA",
    "PRIMER DIAGNOSTICO ",
    "SEGUNDO DIAGNOSTICO ",
    "NOMBRE DE LA OPERACIÓN",
    "REINTERVENCIÓN NO PROG SELECCIONAR LISTA",
    "CODIGO I",
    "CODIGO II ",
    "PPV -GES",
    "TIPO DE CIRUGIA SELECCIONAR LISTA",
    "H: ENTRADA",
    "H: INICIO",
    "H: TERMINO",
    "H: SALIDA",
    "DURACIÓN ",
    "ASEO ",
    "N° PABELLON",
    "SEVICIO SELECCIONAR LISTA",
    "SUSPENDIDA SELECCIONAR LISTA",
]

REEMPLAZAR_FECHAS = {"07-07-215": "07-07-2015"}
REEMPLAZAR_TIPO_DE_OPERACION = {
    "E": "Programada",
    "P": "Programada",
    "EU": "Urgencia",
    "U": "Urgencia",
    "EMERGENCIA": "Urgencia",
    "URGENCIA": "Urgencia",
}

REEMPLAZAR_SUSPENDIDA = {
    "L": "NO",
    "UCI": "NO",
    ",": "NO",
    "S": "NO",
}


def preprocesar_base_de_datos_pabellon(df):
    tmp = df.copy()

    tmp = clean_column_names(tmp)
    tmp = tmp.reset_index(drop=True)
    tmp = tmp.replace(" ", np.nan)
    tmp = tmp.replace("-", np.nan)
    tmp = tmp.dropna(subset="fecha")
    tmp.columns = (
        tmp.columns.str.replace("_seleccionar_lista", "").str.strip("_").str.replace(":", "")
    )
    # Limpia las fechas, ordena por fecha y agrega el anio de la intervencion
    tmp["fecha"] = tmp["fecha"].replace(REEMPLAZAR_FECHAS)
    tmp["fecha"] = pd.to_datetime(tmp["fecha"])
    tmp = tmp.sort_values("fecha")
    tmp["ano_de_intervencion"] = tmp["fecha"].dt.year

    # Limpia tiempos de la operacion
    columnas_horas_en_pabellon = tmp.columns[tmp.columns.str.startswith("h_")]
    for columna_hora in columnas_horas_en_pabellon:
        tmp[columna_hora] = (
            tmp[columna_hora].astype(str).str.replace("1900-01-01 ", "", regex=False)
        )

    tmp["h_inicio"] = limpiar_columna_hora_inicio(tmp["h_inicio"]).dt.time
    tmp["h_termino"] = limpiar_columna_hora_termino(tmp["h_termino"]).dt.time
    tmp["h_salida"] = limpiar_columna_hora_salida(tmp["h_salida"]).dt.time
    tmp["duracion"] = limpiar_columna_duracion(tmp["duracion"]).dt.time
    tmp["aseo"] = limpiar_columna_aseo(tmp["aseo"]).dt.time

    # Limpia codigos de la operacion realizada
    tmp["codigo_i"] = limpiar_codigos_operacion(tmp["codigo_i"])
    tmp["codigo_ii"] = limpiar_codigos_operacion(tmp["codigo_ii"])
    tmp["ppv_-ges"] = limpiar_codigos_operacion(tmp["ppv_-ges"])

    # Agrega columna de la union de los dos codigos de operaciones
    tmp["combinacion_operaciones"] = tmp["codigo_i"] + "_" + tmp["codigo_ii"]

    # Limpia tipo de operacion (Programada, Urgente)
    tmp["tipo_de_cirugia"] = quitar_espacios_y_poner_mayuscula_columna_texto(tmp["tipo_de_cirugia"])
    tmp["cirugia_programada_o_urgente"] = obtener_operaciones_programadas_y_urgentes(
        tmp["tipo_de_cirugia"]
    )

    # Limpia el nombre de la operacion
    tmp["nombre_de_la_operacion"] = quitar_espacios_y_poner_mayuscula_columna_texto(
        tmp["nombre_de_la_operacion"]
    )

    # Limpia los diagnosticos
    tmp["primer_diagnostico"] = quitar_espacios_y_poner_mayuscula_columna_texto(
        tmp["primer_diagnostico"]
    )
    tmp["segundo_diagnostico"] = quitar_espacios_y_poner_mayuscula_columna_texto(
        tmp["segundo_diagnostico"]
    )

    # Limpia la especialidad
    tmp["especialidad"] = quitar_espacios_y_poner_mayuscula_columna_texto(tmp["especialidad"])

    # Limpia los RUTs
    tmp["ficha"] = limpiar_ruts(tmp["ficha"])
    tmp = tmp.rename(columns={"ficha": "ID_PACIENTE"})

    # Limpia la columna de Suspendido
    tmp["suspendida"] = limpiar_suspendida(tmp["suspendida"])

    return tmp


def obtener_operaciones_programadas_y_urgentes(serie_tipo_operacion):
    return serie_tipo_operacion.replace(REEMPLAZAR_TIPO_DE_OPERACION)


def quitar_espacios_y_poner_mayuscula_columna_texto(serie_tipo_operacion):
    return serie_tipo_operacion.str.strip().str.upper()


def limpiar_codigos_operacion(serie_operacion):
    codigos_formateados = (
        serie_operacion.fillna("0000000")
        .astype(str)
        .str.replace("/", "-", regex=False)
        .str.replace(".0", "", regex=False)
        .str.replace(" ", "", regex=False)
    )

    return codigos_formateados


def limpiar_columna_hora_inicio(serie_hora_inicio):
    hora_inicio_reemplazada = serie_hora_inicio.str.replace(
        r"1900-01-01\s|1900-01-05\s", "", regex=True
    ).str.replace(r";|:|\.|_", "", regex=True)

    horas_largo_3 = hora_inicio_reemplazada[hora_inicio_reemplazada.str.len() == 3].index
    hora_inicio_reemplazada[horas_largo_3] = hora_inicio_reemplazada[horas_largo_3].str.pad(
        4, side="left", fillchar="0"
    )

    hora_inicio_reemplazada = hora_inicio_reemplazada.str[:4].str.pad(6, side="right", fillchar="0")

    hora_inicio_reemplazada = pd.to_datetime(
        hora_inicio_reemplazada, format="%H%M%S", errors="coerce"
    )
    return hora_inicio_reemplazada


def limpiar_columna_hora_termino(serie_hora_termino):
    hora_termino_reemplazada = serie_hora_termino.str.replace(
        r"\.|:|-01-06\s|;|_|1900-01-05\s|1900-01-04\s", "", regex=True
    ).replace("1900084500", "084500")

    horas_largo_3 = hora_termino_reemplazada[hora_termino_reemplazada.str.len() == 3].index
    hora_termino_reemplazada[horas_largo_3] = hora_termino_reemplazada[horas_largo_3].str.pad(
        4, side="left", fillchar="0"
    )

    hora_termino_reemplazada = hora_termino_reemplazada.str[:4].str.pad(
        6, side="right", fillchar="0"
    )

    hora_termino_reemplazada = pd.to_datetime(
        hora_termino_reemplazada, format="%H%M%S", errors="coerce"
    )

    return hora_termino_reemplazada


def limpiar_columna_hora_salida(serie_hora_salida):
    return pd.to_datetime(serie_hora_salida.str.replace(":", ""), format="%H%M%S")


def limpiar_columna_duracion(serie_hora_duracion):
    return pd.to_datetime(serie_hora_duracion.astype(str).str.replace(":", ""), format="%H%M%S")


def limpiar_columna_aseo(serie_tiempo_aseo):
    return pd.to_datetime(
        serie_tiempo_aseo.astype(str).replace("0.30", "00:30:00").str.replace(":", ""),
        format="%H%M%S",
    )


def limpiar_ruts(serie_ruts):
    # Deja los RUTs sin DV, sin puntos, sin espacios ni guiones
    ruts = serie_ruts.str.replace("\.|-|\s", "", regex=True).str[:-1]
    # Anonimiza los RUTs
    # ruts = anonimizar_ruts(ruts)

    return ruts


def limpiar_suspendida(serie_suspendidos):
    # Convierte todo a formato unificado
    suspendidos = serie_suspendidos.str.strip().str.upper()

    # Reemplaza glosas poco explicitas
    suspendidos = suspendidos.replace(REEMPLAZAR_SUSPENDIDA)

    # Llena valores faltantes de suspendidos por NO (solamente porque se observo que las int. q.
    # que faltaban, si fueron realizadas)
    # suspendidos = suspendidos.fillna("NO")

    return suspendidos


def clean_column_names(df):
    """
    Cleans the column names of a DataFrame by converting to lowercase and replacing spaces with
    underscores.

    :param df: The input DataFrame.
    :type df: pandas DataFrame

    :return: The DataFrame with cleaned column names.
    :rtype: pandas DataFrame
    """
    tmp = df.copy()

    # Clean and transform the column names using vectorization
    cleaned_columns = (
        df.columns.str.lower()
        .str.strip()
        .str.normalize("NFD")
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
    )
    cleaned_columns = cleaned_columns.str.replace(" ", "_")

    # Assign the cleaned column names back to the DataFrame
    tmp.columns = cleaned_columns

    return tmp


def leer_base_pabellon(input_filepath):
    anios_a_leer = list(filter(lambda x: x.isnumeric(), os.listdir(input_filepath)))
    todos_los_anios_pabellon = []
    for anio in anios_a_leer:
        ruta_carpeta_anio = os.path.join(input_filepath, anio)
        archivos_pabellon = glob.glob(os.path.join(ruta_carpeta_anio, "*.xlsx"))

        df_de_un_anio = pd.concat(
            (
                pd.read_excel(
                    archivo, sheet_name=1, usecols=COLUMNAS_UTILES, dtype={"CODIGO I": str}
                )
                for archivo in archivos_pabellon
            )
        )
        todos_los_anios_pabellon.append(df_de_un_anio)

    df_completa = pd.concat(todos_los_anios_pabellon)

    return df_completa


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    df_completa = leer_base_pabellon(input_filepath)
    df_completa = preprocesar_base_de_datos_pabellon(df_completa)

    df_completa.to_csv(
        f"{output_filepath}/df_procesada.csv", encoding="latin-1", sep=";", index=False
    )


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
