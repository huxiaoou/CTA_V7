import argparse


def parse_args():
    arg_parser = argparse.ArgumentParser(
        description="This project is designed to do a CTA strategy research and backtesting."
    )
    arg_parser.add_argument("--bgn", type=str, help="begin date, format = [YYYYMMDD]", required=True)
    arg_parser.add_argument("--stp", type=str, help="stop  date, format = [YYYYMMDD]")
    arg_parser.add_argument(
        "--nomp",
        default=False,
        action="store_true",
        help="not using multiprocess, for debug. Works only when switch in ('factor', 'signals', 'simulations', 'quick')",
    )
    arg_parser.add_argument(
        "--processes", type=int, default=None, help="number of processes to be called, effective only when nomp = False"
    )
    arg_parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="whether to print more details, effective only when sub function = (feature_selection,)",
    )

    arg_parser_subs = arg_parser.add_subparsers(
        title="Position argument to call sub functions",
        dest="switch",
        description="use this position argument to call different functions of this project. "
        "For example: 'python main.py --bgn 20120104 --stp 20240826 available'",
        required=True,
    )

    # switch: available
    arg_parser_subs.add_parser(name="avlb", help="Calculate available universe")

    # switch: css
    arg_parser_subs.add_parser(name="css", help="Calculate cross section statistics")

    # switch: icov
    arg_parser_subs.add_parser(name="icov", help="Calculate covariance matrix of instruments")

    # switch: market
    arg_parser_subs.add_parser(name="mkt", help="Calculate market index return")

    return arg_parser.parse_args()


if __name__ == "__main__":
    from loguru import logger
    from config import proj_cfg, db_struct_cfg
    from husfort.qlog import define_logger
    from husfort.qcalendar import CCalendar
    from solutions.db_generator import get_avlb_db, get_css_db, get_icov_db, get_market_db

    define_logger()
    calendar = CCalendar(proj_cfg.calendar_path)
    args = parse_args()
    bgn_date, stp_date = args.bgn, args.stp or calendar.get_next_date(args.bgn, shift=1)

    # ---------- databases structure ----------
    db_struct_avlb = get_avlb_db(proj_cfg.avlb_dir)
    db_struct_css = get_css_db(proj_cfg.css_dir, proj_cfg.sectors)
    db_struct_icov = get_icov_db(proj_cfg.icov_dir)
    db_struct_mkt = get_market_db(proj_cfg.mkt_dir, proj_cfg.sectors)

    if args.switch == "avlb":
        from solutions.avlb import main_available

        main_available(
            bgn_date=bgn_date,
            stp_date=stp_date,
            universe=proj_cfg.universe,
            cfg_avlb_unvrs=proj_cfg.avlb_unvrs,
            db_struct_preprocess=db_struct_cfg.preprocess,
            db_struct_avlb=db_struct_avlb,
            calendar=calendar,
        )
    elif args.switch == "css":
        from solutions.css import CCrossSectionCalculator

        css = CCrossSectionCalculator(
            cfg_css=proj_cfg.css,
            db_struct_avlb=db_struct_avlb,
            db_struct_css=db_struct_css,
            db_struct_mkt=db_struct_mkt,
            sectors=proj_cfg.sectors,
        )
        css.main(bgn_date=bgn_date, stp_date=stp_date, calendar=calendar)
    elif args.switch == "icov":
        from solutions.icov import CICOV

        icov = CICOV(
            cfg_icov=proj_cfg.icov,
            universe=proj_cfg.universe,
            db_struct_preprocess=db_struct_cfg.preprocess,
            db_struct_icov=db_struct_icov,
        )
        icov.main(bgn_date=bgn_date, stp_date=stp_date, calendar=calendar)
    elif args.switch == "mkt":
        from solutions.mkt import main_market

        main_market(
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
            db_struct_avlb=db_struct_avlb,
            db_struct_mkt=db_struct_mkt,
            path_mkt_idx_data=proj_cfg.market_index_path,
            mkt_idxes=proj_cfg.mkt.idxes,
            sectors=proj_cfg.sectors,
        )
    else:
        logger.error(f"switch = {args.switch} is not implemented yet.")
