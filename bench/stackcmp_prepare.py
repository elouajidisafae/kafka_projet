"""Prepare pinned images; estimated 5-20 minutes, owner-run command."""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bench.stackcmp_common import prepare
if __name__=="__main__": prepare()
