import os
from itertools import product
from dataclasses import dataclass
from husfort.qsqlite import CDbStruct
from typedefs.typedef_instrus import TUniverse, CCfgAvlbUnvrs
from typedefs.typedef_css import CCfgCss, CCfgICov, CCfgMkt
from typedefs.typedef_returns import CCfgTst, TReturnClass, CRet, TRets


"""
--------------------------------
Part I: generic and project
--------------------------------
"""


@dataclass(frozen=True)
class CCfgDbStruct:
    # --- shared database
    macro: CDbStruct
    forex: CDbStruct
    fmd: CDbStruct
    position: CDbStruct
    basis: CDbStruct
    stock: CDbStruct
    preprocess: CDbStruct
    minute_bar: CDbStruct


@dataclass(frozen=True)
class CCfgConst:
    INIT_CASH: float
    COST_RATE: float
    LAG: int


@dataclass(frozen=True)
class CCfgProj:
    # --- shared
    calendar_path: str
    root_dir: str
    db_struct_path: str
    alternative_dir: str
    market_index_path: str
    by_instru_pos_dir: str
    by_instru_pre_dir: str
    by_instru_min_dir: str
    instru_info_path: str

    # --- project
    project_root_dir: str

    # --- project parameters
    universe: TUniverse
    avlb_unvrs: CCfgAvlbUnvrs
    css: CCfgCss
    icov: CCfgICov
    mkt: CCfgMkt
    const: CCfgConst
    tst: CCfgTst

    @property
    def sectors(self) -> list[str]:
        return sorted(list(set([v.sectorL1 for v in self.universe.values()])))

    @property
    def all_rets(self) -> TRets:
        return [
            CRet(ret_class=TReturnClass(rc), win=w, lag=self.const.LAG)
            for rc, w in product(TReturnClass, self.tst.wins)
        ]

    @property
    def qtest_rets(self) -> TRets:
        return [
            CRet(ret_class=TReturnClass(rc), win=w, lag=self.const.LAG)
            for rc, w in product(TReturnClass, self.tst.wins_qtest)
        ]

    @property
    def avlb_dir(self) -> str:
        return os.path.join(self.project_root_dir, "avlb")

    @property
    def css_dir(self) -> str:
        return os.path.join(self.project_root_dir, "css")

    @property
    def icov_dir(self) -> str:
        return os.path.join(self.project_root_dir, "icov")

    @property
    def mkt_dir(self):
        return os.path.join(self.project_root_dir, "mkt")

    @property
    def test_returns_by_instru_dir(self):
        return os.path.join(self.project_root_dir, "test_returns_by_instru")

    @property
    def test_returns_avlb_raw_dir(self):
        return os.path.join(self.project_root_dir, "test_returns_avlb_raw")
