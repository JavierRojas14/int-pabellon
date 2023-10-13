# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import os
import glob
import pandas as pd
import numpy as np

COLUMNAS_UTILES = [
    "FECHA ",
    "SEXO SELECCIONAR LISTA",
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
]

REEMPLAZAR_FECHAS = {"07-07-215": "07-07-2015"}


def preprocesar_base_de_datos_pabellon(df):
    tmp = df.copy()

    tmp = clean_column_names(tmp)
    tmp = tmp.replace(" ", np.nan)
    tmp = tmp.dropna(subset="fecha")
    tmp.columns = (
        tmp.columns.str.replace("_seleccionar_lista", "").str.strip("_").str.replace(":", "")
    )
    tmp["fecha"] = tmp["fecha"].replace(REEMPLAZAR_FECHAS)
    tmp["fecha"] = pd.to_datetime(tmp["fecha"])
    return tmp


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


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    anios_a_leer = list(filter(lambda x: x.isnumeric(), os.listdir(input_filepath)))
    todos_los_anios_pabellon = []
    for anio in anios_a_leer:
        ruta_carpeta_anio = os.path.join(input_filepath, anio)
        archivos_pabellon = glob.glob(os.path.join(ruta_carpeta_anio, "*.xlsx"))

        df_de_un_anio = pd.concat(
            (
                pd.read_excel(archivo, sheet_name=1, usecols=COLUMNAS_UTILES)
                for archivo in archivos_pabellon
            )
        )
        todos_los_anios_pabellon.append(df_de_un_anio)

    df_completa = pd.concat(todos_los_anios_pabellon)
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
