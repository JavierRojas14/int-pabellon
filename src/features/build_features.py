import pandas as pd


def obtener_diccionario_mai():
    codigos_mai = pd.read_excel(
        "../data/external/2 Arancel MAI 2023(con Res. 244).xls", header=41
    ).iloc[:, [0, 1, 3, 4, 5]]

    codigos_mai.columns = ["CODIGO", "CA", "GLOSA PRESTACION", "FACTURACION TOTAL", "COBRO USUARIO"]
    codigos_mai = codigos_mai.dropna(subset="CODIGO")
    codigos_mai["CODIGO"] = (
        codigos_mai["CODIGO"]
        .astype(str)
        .str.replace(".0", "")
        .str.pad(7, side="left", fillchar="0")
    )

    return codigos_mai
