import os
from husfort.qsqlite import CDbStruct, CSqlTable, CSqlVar


# ----------------------------------------
# ------ sqlite3 database structure ------
# ----------------------------------------


def get_avlb_db(available_dir: str) -> CDbStruct:
    return CDbStruct(
        db_save_dir=available_dir,
        db_name="available.db",
        table=CSqlTable(
            name="available",
            primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
            value_columns=[
                CSqlVar("return", "REAL"),
                CSqlVar("amount", "REAL"),
                CSqlVar("volatility", "REAL"),
                CSqlVar("sectorL0", "TEXT"),
                CSqlVar("sectorL1", "TEXT"),
            ],
        ),
    )
