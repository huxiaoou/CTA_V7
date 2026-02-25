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

    return arg_parser.parse_args()


if __name__ == "__main__":
    from loguru import logger
    from config import proj_cfg, db_struct_cfg
    from husfort.qlog import define_logger
    from husfort.qcalendar import CCalendar
    from solutions.db_generator import get_avlb_db

    define_logger()
    calendar = CCalendar(proj_cfg.calendar_path)
    args = parse_args()
    bgn_date, stp_date = args.bgn, args.stp or calendar.get_next_date(args.bgn, shift=1)
    db_struct_avlb = get_avlb_db(proj_cfg.available_dir)

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
    else:
        logger.error(f"switch = {args.switch} is not implemented yet.")
