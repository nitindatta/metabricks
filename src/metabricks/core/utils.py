from datetime import datetime
import json, time, functools
from typing import List, Dict, Any
import pytz
from string import Template

def log(msg): print(f"[{datetime.datetime.utcnow().isoformat()}] {msg}")

def retry(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for i in range(3):
            try: return func(*args, **kwargs)
            except Exception as e:
                log(f"Attempt {i+1} failed: {e}")
                time.sleep(2**i)
        raise
    return wrapper
  
def resolve_path(template_str: str, extra_vars: Dict[str, str] = None) -> str:
    """
    Resolves placeholders like {{extract_ts_yyyy}}, {{extract_ts_HH}} in the given path template.
    Optionally merges with additional key-value variables (source_system, env, etc.)
    """
    
    now =  datetime.utcnow()
    replacements = {
        "extract_ts_yyyy": f"{now.year:04d}",
        "extract_ts_MM": f"{now.month:02d}",
        "extract_ts_dd": f"{now.day:02d}",
        "extract_ts_HH": f"{now.hour:02d}",
        "extract_ts_mm": f"{now.minute:02d}",
        "extract_ts_ss": f"{now.second:02d}",
        "extract_ts_yyyyMMddHHmmssffffff": now.strftime("%Y%m%d%H%M%S%f"),
    }

    if extra_vars:
        replacements.update(extra_vars)

    # Handle {{ }} and ${ } forms interchangeably
    safe_template = template_str.replace("{{", "${").replace("}}", "}")
    resolved = Template(safe_template).safe_substitute(**replacements)
    return resolved


def audit_log(meta, path):
    return {
        "dataset_id": meta["source_dataset_id"],
        "status": "success",
        "written_to": path,
        "run_ts": datetime.utcnow().isoformat()
    }
