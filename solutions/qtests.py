import os
import numpy as np
import pandas as pd
import multiprocessing as mp
from loguru import logger
from typing import Literal
from rich.progress import Progress, TaskID, TimeElapsedColumn, TimeRemainingColumn, TextColumn, BarColumn
from husfort.qutility import check_and_makedirs, SFG, qtimer, error_handler
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from husfort.qcalendar import CCalendar
from husfort.qplot import CPlotLines
from typedefs.typedef_returns import CRet, TRets
from typedefs.typedef_factors import CCfgFactorGrp
from typedef import TFactorsAvlbDirType, TTestReturnsAvlbDirType
from solutions.test_return import CTestReturnLoader
from solutions.factor import CFactorsLoader
from solutions.db_generator import gen_ic_tests_db, gen_vt_tests_db


class __CQTest:
    def __init__(
        self,
        factor_grp: CCfgFactorGrp,
        ret: CRet,
        factors_avlb_dir: str,
        test_returns_avlb_dir: str,
        tests_dir: str,
    ):
        self.factor_grp = factor_grp
        self.ret = ret
        self.factors_avlb_dir = factors_avlb_dir
        self.test_returns_avlb_dir = test_returns_avlb_dir
        self.tests_dir = tests_dir

    @property
    def save_id(self) -> str:
        return f"{self.factor_grp.factor_class}-{self.ret.ret_name}-{self.factor_grp.decay}"

    def load_returns(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        returns_loader = CTestReturnLoader(
            ret=self.ret,
            test_returns_avlb_dir=self.test_returns_avlb_dir,
        )
        return returns_loader.load(bgn_date, stp_date)

    def load_factors(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        factors_loader = CFactorsLoader(
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
            factors_avlb_dir=self.factors_avlb_dir,
        )
        return factors_loader.load(bgn_date, stp_date)

    def gen_test_db_struct(self) -> CDbStruct:
        raise NotImplementedError

    def save(self, new_data: pd.DataFrame, calendar: CCalendar):
        """

        :param new_data: a pd.DataFrame with columns =
                        ["trade_date"] + self.factor_grp.factor_names
        :param calendar:
        :return:
        """
        test_db_struct = self.gen_test_db_struct()
        check_and_makedirs(test_db_struct.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=test_db_struct.db_save_dir,
            db_name=test_db_struct.db_name,
            table=test_db_struct.table,
            mode="a",
        )
        if sqldb.check_continuity(new_data["trade_date"].iloc[0], calendar) == 0:
            update_data = new_data[test_db_struct.table.vars.names]
            sqldb.update(update_data=update_data)
        return 0

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        test_db_struct = self.gen_test_db_struct()
        check_and_makedirs(test_db_struct.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=test_db_struct.db_save_dir,
            db_name=test_db_struct.db_name,
            table=test_db_struct.table,
            mode="r",
        )
        data = sqldb.read_by_range(
            bgn_date=bgn_date,
            stp_date=stp_date,
            value_columns=["trade_date"] + self.factor_grp.factor_names,
        )
        return data

    def core_for_groupby(self, data: pd.DataFrame, pb: Progress, task: TaskID) -> pd.Series:
        raise NotImplementedError

    def core_for_global(self, input_data: pd.DataFrame, qtest_data: pd.DataFrame) -> pd.DataFrame:
        return qtest_data

    def get_plot_ylim(self) -> tuple[float, float]:
        raise NotImplementedError

    def plot(self, plot_data: pd.DataFrame):
        check_and_makedirs(save_dir := os.path.join(self.tests_dir, "plots"))
        artist = CPlotLines(
            plot_data=plot_data,
            fig_name=f"{self.save_id}",
            fig_save_dir=save_dir,
            colormap="jet",
            line_style=["-", "-."] * int(plot_data.shape[1] / 2),
            line_width=1.2,
        )
        artist.plot()
        artist.set_legend(loc="upper left")
        artist.set_axis_x(xtick_count=20, xtick_label_size=8, xgrid_visible=True)
        artist.set_axis_y(ylim=self.get_plot_ylim(), update_yticklabels=False, ygrid_visible=True)
        artist.save_and_close()
        return 0

    def gen_report(self, test_data: pd.DataFrame, ret_scale: float = 100.0, ann_rate: float = 250) -> pd.DataFrame:
        raise NotImplementedError

    def save_report(self, report: pd.DataFrame, saving_index: bool, float_format: str = "%.6f"):
        check_and_makedirs(save_dir := os.path.join(self.tests_dir, "reports"))
        report_file = f"{self.save_id}.csv"
        report_path = os.path.join(save_dir, report_file)
        report.to_csv(report_path, float_format=float_format, index=saving_index)
        return 0

    def main_cal(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        buffer_bgn_date = calendar.get_next_date(bgn_date, -self.ret.shift)
        iter_dates = calendar.get_iter_list(buffer_bgn_date, stp_date)
        save_dates = iter_dates[self.ret.shift :]
        base_bgn_date, base_stp_date = iter_dates[0], iter_dates[-self.ret.shift]
        returns_data = self.load_returns(base_bgn_date, base_stp_date)
        factors_data = self.load_factors(base_bgn_date, base_stp_date)
        input_data = pd.merge(
            left=returns_data,
            right=factors_data,
            on=["trade_date", "instrument"],
            how="inner",
        )
        lr, lf, li = len(returns_data), len(factors_data), len(input_data)
        if (li != lr) or (li != lf):
            raise ValueError(f"len of factor data = {lf}, len of return data = {lr}, len of input data = {li}.")
        with Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as pb:
            task = pb.add_task(description=f"{self.save_id}")
            pb.update(task_id=task, completed=0, total=len(input_data["trade_date"].unique()))
            qtest_data = input_data.groupby(by="trade_date").apply(
                self.core_for_groupby, pb=pb, task=task  # type:ignore
            )
            qtest_data = self.core_for_global(input_data, qtest_data)

        qtest_data["trade_date"] = save_dates
        new_data = qtest_data[["trade_date"] + self.factor_grp.factor_names]
        new_data = new_data.reset_index(drop=True)
        self.save(new_data, calendar)
        logger.info(f"{self.__class__.__name__} for {SFG(self.save_id)} finished.")
        return 0

    def main_summary(self, bgn_date: str, stp_date: str):
        test_data = self.load(bgn_date, stp_date).set_index("trade_date")
        plot_data = test_data.cumsum()
        self.plot(plot_data=plot_data)
        report = self.gen_report(test_data)
        self.save_report(report, saving_index=False)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        self.main_cal(bgn_date, stp_date, calendar)
        self.main_summary(bgn_date, stp_date)
        return 0


# ----------------------------
# --------- ic-tests ---------
# ----------------------------
class CICTest(__CQTest):
    def core_for_groupby(self, data: pd.DataFrame, pb: Progress, task: TaskID) -> pd.Series:
        s = data[self.factor_grp.factor_names].corrwith(data[self.ret.ret_name], axis=0, method="spearman")
        pb.update(task_id=task, advance=1)
        return s

    def gen_test_db_struct(self) -> CDbStruct:
        return gen_ic_tests_db(
            ic_tests_dir=self.tests_dir,
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
            ret=self.ret,
        )

    def get_plot_ylim(self) -> tuple[float, float]:
        if self.ret.win <= 1:
            ylim = (-40, 80)
        elif self.ret.win <= 5:
            ylim = (-50, 120)
        elif self.ret.win <= 10:
            ylim = (-60, 120)
        else:
            ylim = (-80, 140)
        return ylim

    def gen_report(self, test_data: pd.DataFrame, ret_scale: float = 100.0, ann_rate: float = 250) -> pd.DataFrame:
        test_data["trade_year"] = test_data.index.map(lambda z: z[0:4])
        dfs: list[pd.DataFrame] = []
        for trade_year, trade_year_data in test_data.groupby("trade_year"):
            ic_mean = trade_year_data[self.factor_grp.factor_names].mean()
            ic_std = trade_year_data[self.factor_grp.factor_names].std()
            ir = ic_mean / ic_std
            trade_year_sum = (
                pd.DataFrame(
                    {
                        "trade_year": trade_year,
                        "IC": ic_mean,
                        "IR": ir,
                    }
                )
                .reset_index()
                .rename(columns={"index": "factor"})
            )
            dfs.append(trade_year_sum)
        report = pd.concat(dfs, axis=0, ignore_index=True)
        return report


# ----------------------------
# --------- vt-tests ---------
# ----------------------------
class CVTTest(__CQTest):
    def __init__(self, cost_rate: float, **kwargs):
        super().__init__(**kwargs)
        self.cost_rate = cost_rate

    def core_for_groupby(self, data: pd.DataFrame, pb: Progress, task: TaskID) -> pd.Series:
        s = data[self.ret.ret_name] @ data[self.factor_grp.factor_names] / self.ret.win
        pb.update(task_id=task, advance=1)
        return s

    def core_for_global(self, input_data: pd.DataFrame, qtest_data: pd.DataFrame) -> pd.DataFrame:
        turnover: dict[str, pd.Series] = {}
        for factor in self.factor_grp.factor_names:
            raw_wgt = (
                input_data[["trade_date", "instrument", factor]]
                .pivot_table(index="trade_date", columns="instrument", values=factor, aggfunc="first")
                .fillna(0)
            )
            dlt_wgt = raw_wgt.diff().fillna(0)
            turnover[factor] = dlt_wgt.abs().sum(axis=1)
        turnover_data = pd.DataFrame(turnover)
        return qtest_data - turnover_data * self.cost_rate

    def gen_test_db_struct(self) -> CDbStruct:
        return gen_vt_tests_db(
            vt_tests_dir=self.tests_dir,
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
            ret=self.ret,
        )

    def get_plot_ylim(self) -> tuple[float, float]:
        return -2.0, 2.4

    def gen_report(self, test_data: pd.DataFrame, ret_scale: float = 100.0, ann_rate: float = 250) -> pd.DataFrame:
        test_data["trade_year"] = test_data.index.map(lambda z: z[0:4])
        dfs: list[pd.DataFrame] = []
        for trade_year, trade_year_data in test_data.groupby("trade_year"):
            vt_mean = trade_year_data[self.factor_grp.factor_names].mean() * ret_scale
            vt_std = trade_year_data[self.factor_grp.factor_names].std() * ret_scale
            ann_ret = vt_mean * ann_rate
            ann_vol = vt_std * np.sqrt(ann_rate)
            sharpe = ann_ret / ann_vol
            trade_year_sum = (
                pd.DataFrame(
                    {
                        "trade_year": trade_year,
                        "mean": vt_mean,
                        "std": vt_std,
                        "ann_ret": ann_ret,
                        "ann_vol": ann_vol,
                        "sharpe": sharpe,
                    }
                )
                .reset_index()
                .rename(columns={"index": "factor"})
            )
            dfs.append(trade_year_sum)
        report = pd.concat(dfs, axis=0, ignore_index=True)
        return report


# --------------------------
# --- interface for main ---
# --------------------------
TICTestAuxArgs = tuple[TFactorsAvlbDirType, TTestReturnsAvlbDirType]


@qtimer
def main_qtests(
    rets: TRets,
    factor_grp: CCfgFactorGrp,
    aux_args_list: list[TICTestAuxArgs],
    tests_dir: str,
    bgn_date: str,
    stp_date: str,
    calendar: CCalendar,
    test_type: Literal["ic", "vt"],
    call_multiprocess: bool,
    cost_rate: float,
):
    if test_type == "ic":
        test_cls = CICTest
    elif test_type == "vt":
        test_cls = CVTTest
    else:
        raise ValueError("test_type must be in ['ic', 'vt']")

    tests: list[__CQTest] = []
    for ret in rets:
        for factors_avlb_dir, test_returns_avlb_dir in aux_args_list:
            kwargs = {
                "factor_grp": factor_grp,
                "ret": ret,
                "factors_avlb_dir": factors_avlb_dir,
                "test_returns_avlb_dir": test_returns_avlb_dir,
                "tests_dir": tests_dir,
            }
            if test_type == "vt":
                kwargs.update({"cost_rate": cost_rate})
            test = test_cls(**kwargs)
            tests.append(test)

    if call_multiprocess:
        with mp.get_context("spawn").Pool() as pool:
            for test in tests:
                pool.apply_async(
                    test.main,
                    kwds={
                        "bgn_date": bgn_date,
                        "stp_date": stp_date,
                        "calendar": calendar,
                    },
                    error_callback=error_handler,
                )
            pool.close()
            pool.join()
    else:
        for test in tests:
            test.main(bgn_date, stp_date, calendar)
    return 0
