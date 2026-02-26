import os
from husfort.qsqlite import CDbStruct, CSqlTable, CSqlVar
from typedefs.typedef_returns import TReturnClass, CRet
from typedefs.typedef_factors import TFactorClass, TFactors


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


def gen_test_returns_by_instru_db(
    instru: str,
    test_returns_by_instru_dir: str,
    ret_class: TReturnClass,
    ret: CRet,
) -> CDbStruct:
    """

    :param instru: 'RB.SHFE'
    :param test_returns_by_instru_dir: test_returns_by_instru_dir
    :param ret_class: 'Opn' or 'Cls'
    :param ret:
    :return:
    """
    return CDbStruct(
        db_save_dir=os.path.join(test_returns_by_instru_dir, ret_class),
        db_name=f"{instru}.db",
        table=CSqlTable(
            name=ret.ret_name,
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=[CSqlVar("ticker", "TEXT"), CSqlVar(ret.ret_name, "REAL")],
        ),
    )


def gen_test_returns_avlb_db(
    test_returns_avlb_dir: str,
    ret_class: TReturnClass,
    ret: CRet,
) -> CDbStruct:
    """

    :param test_returns_avlb_dir: 'raw' or 'neu'
    :param ret_class: 'Opn' or 'Cls'
    :param ret:
    :return:
    """

    return CDbStruct(
        db_save_dir=test_returns_avlb_dir,
        db_name=f"{ret_class}.db",
        table=CSqlTable(
            name=ret.ret_name,
            primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
            value_columns=[CSqlVar(ret.ret_name, "REAL")],
        ),
    )


def gen_factors_by_instru_db(
    instru: str,
    factors_by_instru_dir: str,
    factor_class: TFactorClass,
    factors: TFactors,
) -> CDbStruct:
    """

    :param instru: 'RB.SHFE'
    :param factors_by_instru_dir: factors_by_instru_dir
    :param factor_class:
    :param factors:
    :return:
    """
    return CDbStruct(
        db_save_dir=os.path.join(factors_by_instru_dir, factor_class),
        db_name=f"{instru}.db",
        table=CSqlTable(
            name="factor",
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=[CSqlVar("ticker", "TEXT")] + [CSqlVar(fac.factor_name, "REAL") for fac in factors],
        ),
    )


def gen_factors_avlb_db(
    factors_avlb_dir: str,
    factor_class: TFactorClass,
    factors: TFactors,
) -> CDbStruct:
    """

    :param factors_avlb_dir: 'raw' or 'ewa'
    :param factor_class:
    :param factors:
    :return:
    """

    return CDbStruct(
        db_save_dir=factors_avlb_dir,
        db_name=f"{factor_class}.db",
        table=CSqlTable(
            name="factor",
            primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
            value_columns=[CSqlVar(fac.factor_name, "REAL") for fac in factors],
        ),
    )


def gen_ic_tests_db(
    ic_tests_dir: str,
    factor_class: TFactorClass,
    factors: TFactors,
    ret: CRet,
) -> CDbStruct:
    """

    :param ic_tests_dir:
    :param factor_class:
    :param factors:
    :param ret:
    :return:
    """

    db_name = f"{factor_class}-{ret.ret_name}.db"
    return CDbStruct(
        db_save_dir=os.path.join(ic_tests_dir, "data"),
        db_name=db_name,
        table=CSqlTable(
            name="ic",
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=[CSqlVar(fac.factor_name, "REAL") for fac in factors],
        ),
    )


def gen_vt_tests_db(
    vt_tests_dir: str,
    factor_class: TFactorClass,
    factors: TFactors,
    ret: CRet,
) -> CDbStruct:
    """

    :param vt_tests_dir:
    :param factor_class:
    :param factors:
    :param ret:
    :return:
    """

    db_name = f"{factor_class}-{ret.ret_name}.db"
    return CDbStruct(
        db_save_dir=os.path.join(vt_tests_dir, "data"),
        db_name=db_name,
        table=CSqlTable(
            name="vt",
            primary_keys=[CSqlVar("trade_date", "TEXT")],
            value_columns=[CSqlVar(fac.factor_name, "REAL") for fac in factors],
        ),
    )
