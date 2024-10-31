import setuptools

setuptools.setup(
    name="ecomm-cltv-spark-pkg",
    version="1.0.0",
    author="Reynolds Pravindev",
    package_dir={"": "cltv"},
    packages=setuptools.find_packages(where="cltv"),
    install_requires=['ucimlrepo'],
    entry_points={'console_scripts': ['adf_rest_api_cli=adf_rest_api.main:main']},
)
