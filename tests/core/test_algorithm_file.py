import json
from pathlib import Path

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.core.algorithm_file import AlgorithmFile


def write_algorithm(path: Path) -> None:
    path.write_text(
        "from pydantic import BaseModel\n"
        "from ziplime.config.base_algorithm_config import BaseAlgorithmConfig\n"
        "\n"
        "class EquityToTrade(BaseModel):\n"
        "    symbol: str\n"
        "    target_percentage: float\n"
        "\n"
        "class AlgorithmConfig(BaseAlgorithmConfig):\n"
        "    currency: str\n"
        "    equities_to_trade: list[EquityToTrade]\n"
    )


def test_algorithm_file_parses_custom_nested_configuration(tmp_path: Path):
    algorithm_file = tmp_path / "algorithm.py"
    config_file = tmp_path / "config.json"
    write_algorithm(algorithm_file)
    config_file.write_text(json.dumps({
        "currency": "USD",
        "equities_to_trade": [
            {"symbol": "AAPL", "target_percentage": 10},
            {"symbol": "NVDA", "target_percentage": 25.5},
        ],
    }))

    algorithm = AlgorithmFile(
        algorithm_file=str(algorithm_file),
        algorithm_config_file=str(config_file),
    )

    assert algorithm.config.currency == "USD"
    assert [(item.symbol, item.target_percentage) for item in algorithm.config.equities_to_trade] == [
        ("AAPL", 10.0),
        ("NVDA", 25.5),
    ]


def test_algorithm_file_uses_base_configuration_without_config_file(tmp_path: Path):
    algorithm_file = tmp_path / "algorithm.py"
    write_algorithm(algorithm_file)

    algorithm = AlgorithmFile(algorithm_file=str(algorithm_file))

    assert type(algorithm.config) is BaseAlgorithmConfig
