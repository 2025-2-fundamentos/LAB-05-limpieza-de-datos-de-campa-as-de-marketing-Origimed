"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    import glob
    import zipfile

    import pandas as pd

    # Carpeta de entrada y de salida
    input_dir = os.path.join("files", "input")
    output_dir = os.path.join("files", "output")
    os.makedirs(output_dir, exist_ok=True)


    frames = []
    pattern = os.path.join(input_dir, "bank-marketing-campaing-*.csv.zip")
    for zip_path in sorted(glob.glob(pattern)):
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                frames.append(pd.read_csv(f))

    if not frames:
        return

    data = pd.concat(frames, ignore_index=True)

    if "Unnamed: 0" in data.columns:
        data = data.drop(columns=["Unnamed: 0"])


    if "job" in data.columns:
        data["job"] = (
            data["job"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace("-", "_", regex=False)
        )

    if "education" in data.columns:
        data["education"] = data["education"].astype(str).str.replace(
            ".", "_", regex=False
        )

    if "credit_default" in data.columns:
        data["credit_default"] = (
            data["credit_default"]
            .astype(str)
            .map({"yes": 1, "no": 0, "unknown": 0})
            .astype(int)
        )

    if "mortgage" in data.columns:
        data["mortgage"] = (
            data["mortgage"]
            .astype(str)
            .map({"yes": 1, "no": 0, "unknown": 0})
            .astype(int)
        )

    if "previous_outcome" in data.columns:
        data["previous_outcome"] = (
            data["previous_outcome"]
            .astype(str)
            .map({"success": 1, "failure": 0, "nonexistent": 0})
            .astype(int)
        )

    if "campaign_outcome" in data.columns:
        data["campaign_outcome"] = (
            data["campaign_outcome"]
            .astype(str)
            .map({"yes": 1, "no": 0})
            .astype(int)
        )

    if {"month", "day"}.issubset(data.columns):
        month_map = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }

        month_num = data["month"].astype(str).str.lower().map(month_map)

        data["last_contact_date"] = pd.to_datetime(
            {
                "year": 2022,
                "month": month_num,
                "day": data["day"].astype(int),
            },
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")


    # Campaign
    campaign_cols = [
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "last_contact_date",
    ]
    campaign = data[campaign_cols].copy()
    campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # Economics
    economics_cols = ["client_id", "cons_price_idx", "euribor_three_months"]
    economics = data[economics_cols].copy()
    economics.to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    # Client
    client_cols = [
        "client_id",
        "age",
        "job",
        "marital",
        "education",
        "credit_default",
        "mortgage",
    ]
    client = data[client_cols].copy()
    client.to_csv(os.path.join(output_dir, "client.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()
