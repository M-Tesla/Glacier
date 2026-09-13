"""Copy libglacier built by Zig into the wheel. The user of a built wheel does not need Zig."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop

try:
    from setuptools.command.bdist_wheel import bdist_wheel
except ImportError:
    from wheel.bdist_wheel import bdist_wheel


def _lib_name() -> str:
    if sys.platform == "darwin":
        return "libglacier.dylib"
    if sys.platform == "win32":
        return "glacier.dll"
    return "libglacier.so"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def copy_lib() -> None:
    root = _repo_root()
    name = _lib_name()
    src = Path(os.environ["GLACIER_LIB"]) if os.environ.get("GLACIER_LIB") else root / "zig-out" / "lib" / name
    dest = Path(__file__).parent / "glacier" / name
    if not src.is_file() and (root / "build.zig").is_file():
        zig = os.environ.get("ZIG", "zig")
        subprocess.check_call([zig, "build", "-Doptimize=ReleaseFast", "-Dlib_only=true"], cwd=root)
        src = root / "zig-out" / "lib" / name
    if not src.is_file():
        raise FileNotFoundError(
            f"missing {src}; build Glacier with $ZIG build -Dlib_only=true or set GLACIER_LIB"
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


class BuildPy(build_py):
    def run(self):
        copy_lib()
        super().run()


class Develop(develop):
    def run(self):
        copy_lib()
        super().run()


class BdistWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):
        python, abi, plat = super().get_tag()
        return "py3", "none", plat


setup(cmdclass={"build_py": BuildPy, "develop": Develop, "bdist_wheel": BdistWheel})
