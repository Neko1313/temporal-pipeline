from pathlib import Path
from core.yaml_loader import YAMLConfigParser

loader = YAMLConfigParser().parse_file(
    Path("examples/simple_etl.yml")
)

print(loader)