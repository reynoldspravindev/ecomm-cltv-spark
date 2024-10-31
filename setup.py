import setuptools

setuptools.setup(
    name="ecomm-cltv-spark-pkg",
    version="1.0.0",
    author="Reynolds Pravindev",
    package_dir={"": "cltv"},
    packages=setuptools.find_packages(where="cltv"),
    install_requires=['ucimlrepo'],
    entry_points={'console_scripts': ['calc_cltv=cltv.main:main']},
)
