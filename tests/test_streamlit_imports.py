import subprocess
import sys
from pathlib import Path


def test_app_package_imports_with_streamlit_script_directory_first():
    root = Path(__file__).resolve().parents[1]
    # Streamlit puts the directory containing ui.py first on sys.path.
    result = subprocess.run(
        [sys.executable, "-c", (
            "import sys; "
            "sys.path.insert(0, 'app'); "
            "from app import bootstrap, settings"
        )],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
