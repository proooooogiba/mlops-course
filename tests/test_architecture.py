import ast
import subprocess
import sys
from pathlib import Path


def test_core_depends_only_on_standard_library_and_inner_layers():
    for layer in ("domain", "application"):
        allowed = ["src.domain"]
        if layer == "application":
            allowed.append("src.application")
        for file in Path("src", layer).rglob("*.py"):
            tree = ast.parse(file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    modules = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    assert node.level == 0, f"Use explicit layer imports in {file}"
                    modules = [node.module]
                else:
                    continue
                for module in modules:
                    assert module.split(".")[0] in sys.stdlib_module_names or any(module == prefix or module.startswith(prefix + ".") for prefix in allowed), f"Forbidden dependency in {file}: {module}"


def test_core_imports_without_site_packages():
    subprocess.run([sys.executable, "-S", "-c", "from src.domain.entities import FraudPolicy; from src.application.use_cases import ScoreTransaction, GetResults"], check=True)
