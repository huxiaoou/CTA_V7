from dataclasses import dataclass


@dataclass(frozen=True)
class CCfgCss:
    vma_win: int
    vma_threshold: float
    vma_wgt: float
    sev_win: int

    @property
    def buffer_win(self) -> int:
        return max(self.vma_win, self.sev_win)


@dataclass(frozen=True)
class CCfgICov:
    win: int


@dataclass(frozen=True)
class CCfgMkt:
    equity: str
    commodity: str

    @property
    def idxes(self) -> list[str]:
        return [self.equity, self.commodity]
