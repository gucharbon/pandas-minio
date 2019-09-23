# pandas-minio

0) Start minio with docker

> Note: You can use shell scripts provided in this repo or do as you want

1) Install requirements in a virtual environment:

```python
virtualenv venv
. venv/bin/activate
pip install pandas numpy minio pyarrow
```

2) Install kernel for jupyter and start notebook:
```python
. venv/bin/activate
pip install ipykernel
python -m ipykernel install --user --name "demo-pandas-minio"
jupyter notebook
``` 

3) Run all cells from notebook `subscribe_dataframes.ipynb`

4) Run all cells from notebook `pandas_to_minio.ipynb`


