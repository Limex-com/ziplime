"""Makes the shared test fixtures importable without installing the tests as a package."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
