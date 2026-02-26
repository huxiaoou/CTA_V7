import yaml
from husfort.qutility import check_and_mkdir
from husfort.qsqlite import CDbStruct, CSqlTable
from typedefs.typedef_instrus import TUniverse, TInstruName, CCfgInstru, CCfgAvlbUnvrs
from typedefs.typedef_css import CCfgCss, CCfgICov, CCfgMkt
from typedefs.typedef_returns import CCfgTst
from typedef import CCfgProj, CCfgDbStruct, CCfgConst
from solutions.factor import CCfgFactors

# ---------- project configuration ----------

with open("config.yaml", "r") as f:
    _config = yaml.safe_load(f)

universe = TUniverse({TInstruName(k): CCfgInstru(**v) for k, v in _config["universe"].items()})

proj_cfg = CCfgProj(
    # --- shared data path
    calendar_path=_config["path"]["calendar_path"],
    root_dir=_config["path"]["root_dir"],
    db_struct_path=_config["path"]["db_struct_path"],
    alternative_dir=_config["path"]["alternative_dir"],
    market_index_path=_config["path"]["market_index_path"],
    by_instru_pos_dir=_config["path"]["by_instru_pos_dir"],
    by_instru_pre_dir=_config["path"]["by_instru_pre_dir"],
    by_instru_min_dir=_config["path"]["by_instru_min_dir"],
    instru_info_path=_config["path"]["instru_info_path"],
    # --- project data root dir
    project_root_dir=_config["path"]["project_root_dir"],
    # --- global settings
    universe=universe,
    avlb_unvrs=CCfgAvlbUnvrs(**_config["available"]),
    css=CCfgCss(**_config["css"]),
    icov=CCfgICov(**_config["icov"]),
    mkt=CCfgMkt(**_config["mkt"]),
    const=CCfgConst(**_config["CONST"]),
    tst=CCfgTst(**_config["tst"]),
)

check_and_mkdir(proj_cfg.project_root_dir)

# --- factors ---
cfg_factors = CCfgFactors(
    algs_dir="factor_algs_activated",
    cfg_data=_config["factors"],
    decay=_config["factor_decay_default"],
)

# ---------- databases structure ----------
with open(proj_cfg.db_struct_path, "r") as f:
    _db_struct = yaml.safe_load(f)

db_struct_cfg = CCfgDbStruct(
    macro=CDbStruct(
        db_save_dir=proj_cfg.alternative_dir,
        db_name=_db_struct["macro"]["db_name"],
        table=CSqlTable(cfg=_db_struct["macro"]["table"]),
    ),
    forex=CDbStruct(
        db_save_dir=proj_cfg.alternative_dir,
        db_name=_db_struct["forex"]["db_name"],
        table=CSqlTable(cfg=_db_struct["forex"]["table"]),
    ),
    fmd=CDbStruct(
        db_save_dir=proj_cfg.root_dir,
        db_name=_db_struct["fmd"]["db_name"],
        table=CSqlTable(cfg=_db_struct["fmd"]["table"]),
    ),
    position=CDbStruct(
        db_save_dir=proj_cfg.by_instru_pos_dir,
        db_name=_db_struct["position"]["db_name"],
        table=CSqlTable(cfg=_db_struct["position"]["table"]),
    ),
    basis=CDbStruct(
        db_save_dir=proj_cfg.root_dir,
        db_name=_db_struct["basis"]["db_name"],
        table=CSqlTable(cfg=_db_struct["basis"]["table"]),
    ),
    stock=CDbStruct(
        db_save_dir=proj_cfg.root_dir,
        db_name=_db_struct["stock"]["db_name"],
        table=CSqlTable(cfg=_db_struct["stock"]["table"]),
    ),
    preprocess=CDbStruct(
        db_save_dir=proj_cfg.by_instru_pre_dir,
        db_name=_db_struct["preprocess"]["db_name"],
        table=CSqlTable(cfg=_db_struct["preprocess"]["table"]),
    ),
    minute_bar=CDbStruct(
        db_save_dir=proj_cfg.by_instru_min_dir,
        db_name=_db_struct["fMinuteBar"]["db_name"],
        table=CSqlTable(cfg=_db_struct["fMinuteBar"]["table"]),
    ),
)

if __name__ == "__main__":
    print("--- Project Configuration ---")
    print(proj_cfg)
    print("--- Factors ---")
    print(cfg_factors)
