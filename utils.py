import os
import sys
import warnings
import datetime

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from sqlalchemy import create_engine


class Tee:
    # stdout을 화면과 파일에 동시에 출력
    def __init__(self, path: str):
        self._stdout = sys.stdout
        self._file = open(path, "w", encoding="utf-8")
        sys.stdout = self

    def write(self, msg: str):
        self._stdout.write(msg)
        self._file.write(msg)

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def close(self):
        sys.stdout = self._stdout
        self._file.close()


def make_log(out_dir: str) -> tuple[str, Tee]:
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return ts, Tee(f"{out_dir}/run_{ts}.txt")


def get_engine(url: str | None = None):
    url = url or os.getenv("MYSQL_URL")
    if not url:
        raise ValueError("MYSQL_URL 환경변수가 설정되지 않았습니다.")
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def setup_plt():
    candidates = ["NanumGothic", "Malgun Gothic", "AppleGothic", "Noto Sans KR"]
    available = {f.name for f in fm.fontManager.ttflist}
    font = next((f for f in candidates if f in available), None)

    if font:
        plt.rc("font", family=font)

    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams.update({
        "figure.dpi": 100,
        "savefig.dpi": 150,
        "savefig.bbox": "tight",
    })


def init():
    warnings.filterwarnings("ignore")
    setup_plt()
