from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def to_jsonable(obj: Any) -> Any:
    # Best-effort conversion for reporting/LLM prompts
    try:
        return json.loads(json.dumps(obj))
    except Exception:
        pass

    if hasattr(obj, "to_dict"):
        try:
            return obj.to_dict()
        except Exception:
            pass

    try:
        return asdict(obj)  # dataclass
    except Exception:
        pass

    return str(obj)


def runtime_provenance() -> dict[str, str]:
    """Versions and timestamp for report footer."""
    import sys
    from datetime import datetime, timezone

    import numpy as np
    import pandas as pd

    out: dict[str, str] = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "python": sys.version.split()[0],
        "pandas": pd.__version__,
        "numpy": np.__version__,
    }
    try:
        import matplotlib

        out["matplotlib"] = matplotlib.__version__
    except Exception:
        pass
    try:
        import seaborn as sns

        out["seaborn"] = sns.__version__
    except Exception:
        pass
    try:
        import scipy

        out["scipy"] = scipy.__version__
    except Exception:
        pass
    return out

