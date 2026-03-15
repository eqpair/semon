import json
import logging
import os
import numpy as np
from datetime import datetime, time
from pathlib import Path

logger = logging.getLogger(__name__)

# 저장소 루트 경로
REPO_PATH = Path(os.environ.get("SEMON_REPO_PATH", "/home/eq/semon"))
DATA_PATH = REPO_PATH / "docs" / "data"
SIGNALS_FILE = DATA_PATH / "signals.json"


# ── 시장 시간 확인 ────────────────────────────────────────────

def is_market_time() -> bool:
    """평일 09:00 ~ 15:30 여부 확인"""
    now = datetime.now()
    weekday = now.weekday()
    current = now.time()

    is_weekday   = 0 <= weekday <= 4
    is_open_hour = time(9, 0) <= current <= time(15, 30)

    return is_weekday and is_open_hour


# ── JSON 저장 ─────────────────────────────────────────────────

class _SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return None if (np.isnan(obj) or np.isinf(obj)) else float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, float) and (obj != obj or obj == float("inf") or obj == float("-inf")):
            return None
        return super().default(obj)


def save_json(data: dict, path: Path = SIGNALS_FILE) -> bool:
    """
    JSON을 임시 파일에 먼저 쓴 뒤 원자적으로 교체
    기존 stmon의 safe_json_dump 패턴과 동일
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(path) + ".tmp")

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, cls=_SafeEncoder)
            f.flush()
            os.fsync(f.fileno())

        tmp.replace(path)
        logger.info(f"JSON 저장 완료: {path}")
        return True

    except Exception as e:
        logger.error(f"JSON 저장 실패 ({path}): {e}")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


# ── Git push ──────────────────────────────────────────────────

def git_push(commit_msg: str = None) -> bool:
    """
    signals.json을 GitHub에 push
    기존 stmon과 동일하게 gitpython 사용
    """
    try:
        import git
        repo = git.Repo(REPO_PATH)

        # 변경사항 없으면 스킵
        if not repo.is_dirty(untracked_files=True):
            logger.info("변경사항 없음, push 스킵")
            return True

        repo.index.add(["docs/data/signals.json"])

        msg = commit_msg or f"update {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        repo.index.commit(msg)

        origin = repo.remote(name="origin")
        origin.push()

        logger.info(f"git push 완료: {msg}")
        return True

    except Exception as e:
        logger.error(f"git push 실패: {e}")
        return False


def save_and_push(data: dict) -> bool:
    """JSON 저장 + git push 한번에"""
    saved = save_json(data)
    if not saved:
        return False
    return git_push()

CLOSING_FILE = DATA_PATH / "signals_closing.json"

def save_closing(data: dict) -> bool:
    """장 마감 시 closing 스냅샷 저장"""
    return save_json(data, CLOSING_FILE)