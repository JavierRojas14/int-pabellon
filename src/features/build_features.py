import pandas as pd


def obtener_diccionario_mai():
    codigos_mai = pd.read_excel(
        "../data/external/2 Arancel MAI 2023(con Res. 244).xls", header=6
    ).iloc[:, [0, 1, 3, 4, 5]]

    codigos_mai.columns = ["CODIGO", "CA", "GLOSA PRESTACION", "FACTURACION TOTAL", "COBRO USUARIO"]
    codigos_mai = codigos_mai.dropna(subset="CODIGO")

    return codigos_mai
