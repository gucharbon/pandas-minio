# pandas-minio

0. Start minio with docker

You can use shell scripts provided in this repo or do as you want. If using shell scripts, simply run:

```shell
./create_secret.sh
./start_minio.sh
```

> You will be able to connect to minio using access_key: `"access_key"` and secret_key: `"secret_key"`.

1. Install requirements in a virtual environment:

```shell
virtualenv venv
. venv/bin/activate
pip install pandas numpy minio pyarrow
```

2. Install kernel for jupyter and start notebook:
```shell
. venv/bin/activate
pip install ipykernel
python -m ipykernel install --user --name "demo-pandas-minio"
jupyter notebook
``` 

3. Run all cells from notebook `subscribe_dataframes.ipynb`

4. Run all cells from notebook `pandas_to_minio.ipynb`


