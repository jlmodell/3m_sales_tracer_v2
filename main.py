import csv
import re

from constants import ITEMS
from db_connection import dwh

__month__ = input("Month: >\t").upper()
__year__ = input("Year: >\t")

distributors = {
    "mgm": f"MCKESSON-{__month__}-{__year__}",
    "cardinal": f"CARDINAL-{__month__}-{__year__}",
    "medline": f"MEDLINE-{__month__}-{__year__}",
    "concordance": f"CONCORDANCE-{__month__}-{__year__}",
}


def process_row(row: dict) -> list:
    row = list(row.values())
    temp = {}

    if row[2] == "BUSSE" and any(
        [
            "CARDINAL" in row[9],
            "MCKESSON" in row[9],
            "MEDLINE" in row[9],
            "CONCORDANCE" in row[9],
            "SENECA" in row[9],
            "MIDWEST" in row[9],
        ]
    ):
        return []

    temp["name"] = row[2]

    temp["branch/div"] = ""
    if row[19].get("branch_id", None) is not None:
        temp["branch/div"] = row[19].get("branch_id", "")
    elif row[19].get("unique_ship_to_id", None) is not None:
        temp["branch/div"] = row[19].get("unique_ship_to_id", "")
    elif row[19].get("sold_to_dea", None) is not None:
        temp["branch/div"] = row[19].get("sold_to_dea", "")

    temp["cust item"] = str(row[4])
    temp["unique ID"] = str(row[0])
    temp["eu name"] = row[9]
    temp["eu addr1"] = row[11]
    temp["eu addr2"] = row[12]
    temp["eu city"] = row[13]
    temp["eu state"] = row[14]
    temp["eu zip"] = row[15]

    temp["invoice #"] = ""
    if row[19].get("so", None) is not None:
        temp["invoice #"] = row[19].get("so", "")
    elif row[19].get("invoice", None) is not None:
        temp["invoice #"] = row[19].get("invoice", "")

    temp["date"] = row[8].strftime("%m/%d/%Y")
    temp["busse item"] = str(row[4])
    temp["sale uom"] = "CS" if row[7] in ["CS", "CA"] else "EA"
    temp["qty"] = int(row[6])

    if ITEMS[str(row[4])][4] != 0:
        if row[7] in ["CS", "CA"]:
            temp["uom eaches"] = ITEMS[str(row[4])][4]
            temp["3m product"] = ITEMS[str(row[4])][3]
            temp["3m qty/sale"] = ITEMS[str(row[4])][4] * float(row[6])
            temp["ext dist cost"] = (
                ITEMS[str(row[4])][4] * ITEMS[str(row[4])][5] * float(row[6])
            )
        else:
            temp["uom eaches"] = "1"
            temp["3m product"] = ITEMS[str(row[4])][4]
            temp["3m qty/sale"] = float(row[6])
            temp["ext dist cost"] = ITEMS[str(row[4])][5] * float(row[6])
    else:
        if row[7] in ["CS", "CA"]:
            temp["uom eaches"] = ITEMS[str(row[4])][1]
            temp["3m product"] = ITEMS[str(row[4])][0]
            temp["3m qty/sale"] = ITEMS[str(row[4])][1] * float(row[6])
            temp["ext dist cost"] = (
                ITEMS[str(row[4])][1] * ITEMS[str(row[4])][2] * float(row[6])
            )
        else:
            temp["uom eaches"] = "1"
            temp["3m product"] = ITEMS[str(row[4])][0]
            temp["3m qty/sale"] = float(row[6])
            temp["ext dist cost"] = ITEMS[str(row[4])][2] * float(row[6])

    return list(temp.values())


def process_docs() -> list:
    output = []
    key_regex = re.compile(
        rf"^(BUSSE|CARDINAL|MEDLINE|MCKESSON|CONCORDANCE)-{__month__}-{__year__}$",
        re.IGNORECASE,
    )
    for doc in list(
        dwh.find(
            {"key": key_regex, "item": {"$in": [str(item) for item in ITEMS.keys()]}}
        )
    ):
        processed = process_row(doc)
        if processed:
            output.append(processed)

    return output


def write_csv(data: list) -> None:
    with open(
        f"{__month__.title()} {__year__} 3M Sales Tracings.csv", mode="w+", newline=""
    ) as output:
        out = csv.writer(
            output,
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )
        out.writerow(
            [
                "Customer Info.",
                "",
                "",
                "End User Information",
                "",
                "",
                "",
                "",
                "",
                "",
                "Sales Information",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        out.writerow(
            [
                "Name",
                "Branch/Div.",
                "Cust Item #",
                "Unique Id",
                "Name",
                "Addr1",
                "Addr2",
                "City",
                "State",
                "Zip",
                "Inv #.",
                "",
                "Busse Item",
                "Sale UOM",
                "Qty",
                "UOM Eaches",
                "3M Product",
                "3M Qty/Sale",
                "Extended Distributor Cost",
            ]
        )

        for row in data:
            out.writerow(row)


if __name__ == "__main__":
    write_csv(process_docs())
