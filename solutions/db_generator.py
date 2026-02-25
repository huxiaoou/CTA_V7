import os
from husfort.qsqlite import CDbStruct, CSqlTable, CSqlVar


# ----------------------------------------
# ------ sqlite3 database structure ------
# ----------------------------------------


def get_avlb_db(available_dir: str) -> CDbStruct:
    return CDbStruct(
        db_save_dir=available_dir,
        db_name="avlb.db",
        table=CSqlTable(
            name="avlb",
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


def get_css_db(cross_section_stats_dir: str, sectors: list[str]) -> CDbStruct:
    others = [CSqlVar(f"volatility_{z}", "REAL") for z in sectors] + [CSqlVar("volatility_sector", "REAL")]
    return CDbStruct(
        db_save_dir=cross_section_stats_dir,
        db_name="css.db",
        table=CSqlTable(
            name="css",
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=[
                CSqlVar("volatility", "REAL"),
                CSqlVar("dispersion", "REAL"),
                CSqlVar("skewness", "REAL"),
                CSqlVar("kurtosis", "REAL"),
                CSqlVar("vma", "REAL"),
                CSqlVar("dma", "REAL"),
                CSqlVar("sma", "REAL"),
                CSqlVar("kma", "REAL"),
                CSqlVar("tot_wgt", "REAL"),
                CSqlVar("sev", "REAL"),  # ratio of Significant Eigen Values
                CSqlVar("dcov", "REAL"),  # difference of co-variance
            ]
            + others,
        ),
    )


def get_icov_db(icov_db_dir: str) -> CDbStruct:
    return CDbStruct(
        db_save_dir=icov_db_dir,
        db_name="icov.db",
        table=CSqlTable(
            name="icov",
            primary_keys=[
                CSqlVar("trade_date", "TEXT"),
                CSqlVar("instrument0", "TEXT"),
                CSqlVar("instrument1", "TEXT"),
            ],
            value_columns=[CSqlVar("cov", "REAL")],
        ),
    )


def get_market_db(market_dir: str, sectors: list[str]) -> CDbStruct:
    v_s0 = [CSqlVar("market", "REAL"), CSqlVar("C", "REAL")]
    v_s1 = [CSqlVar(s, "REAL") for s in sectors]
    v_idx = [CSqlVar("INH0100_NHF", "REAL"), CSqlVar("I881001_WI", "REAL")]
    return CDbStruct(
        db_save_dir=market_dir,
        db_name="mkt.db",
        table=CSqlTable(
            name="mkt",
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=v_s0 + v_s1 + v_idx,
        ),
    )
