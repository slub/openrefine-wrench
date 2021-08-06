from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")
license = (here / "LICENSE").read_text(encoding="utf-8")

setup(
    name="openrefine-wrench",
    version="0.0.1",
    description="commandline tool to manage basic openrefine ralted tasks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/slub/openrefine-wrench",
    author="Thomas Gersch",
    author_email="thomas.gersch@slub-dresden.de",
    license = license,
    packages=find_packages(exclude=('tests', 'docs')),
    python_requires=">=3.6, <4",
    install_requires=open("requirements.txt").read().split("\n"),
    entry_points={
        "console_scripts": [
            "openrefine-wrench=openrefine_wrench.openrefine_wrench:openrefine_wrench",
            "openrefine-wrench-create=openrefine_wrench.openrefine_wrench:openrefine_wrench_create",
            "openrefine-wrench-apply=openrefine_wrench.openrefine_wrench:openrefine_wrench_apply",
            "openrefine-wrench-export=openrefine_wrench.openrefine_wrench:openrefine_wrench_export",
            "openrefine-wrench-delete=openrefine_wrench.openrefine_wrench:openrefine_wrench_delete",
        ],
    },
)
