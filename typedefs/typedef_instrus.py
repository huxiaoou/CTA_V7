from dataclasses import dataclass


@dataclass(frozen=True)
class CCfgInstru:
    sectorL0: str
    sectorL1: str


TInstruName = str
TUniverse = dict[TInstruName, CCfgInstru]


@dataclass(frozen=True)
class CCfgAvlbUnvrs:
    win: int
    amount_threshold: float
    win_vol: int
    win_vol_min: int

    @property
    def buffer_win(self) -> int:
        return max(self.win, self.win_vol, self.win_vol_min)

    @property
    def wins_volatility(self) -> tuple[int, int]:
        return self.win_vol, self.win_vol_min
