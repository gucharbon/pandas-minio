"""
@copyright: Guillaume Charbonnier <guillaume.charbonnier@capgemini>

This module defines the `PandasBucket` class which can be used to read/store pandas objects (`pandas.DataFrame` or `pandas.Series`)
from/into minio or S3 compatible storage. It can also be used to subscribe to created objects and receive pandas object directly.
"""
from typing import Union, List, Generator, Dict
import io
import minio
# Note: pyarrow is required by pandas and must be installed.
import pandas

client = minio.Minio(endpoint="134.209.85.215:9001", access_key="access_key", secret_key="secret_key", secure=False)


class PandasBucket:
    """
    A class that allows to perform operations on a unique bucket in minio or any S3 compatible storage.
    It is possible to insert pandas object into desired format (feather, parquet, json, csv).
    It is also possible to listen to events or subscribe to new datasets added in the bucket.
    It requires a `minio.Minio` client as well as the name of the bucket.
    """
    def __init__(self, client: minio.Minio, name: str = "datasets"):
        self.client = client
        self.name = name
        self.create()
    
    @property
    def exists(self) -> bool:
        """ Return True if bucket exists, else False. """
        return self.client.bucket_exists(self.name)
    
    def create(self) -> None:
        """ Create the bucket if it does not exist. """
        if not self.exists:
            self.client.make_bucket(name)
    
    def destroy(self) -> None:
        """ Destroy the bucket as well as all of its content. """
        if self.exists:
            self.client.remove_bucket(self.name)
    
    def __put__(self, name: str, length: int, data: io.BytesIO) -> str:
        """ Put an object into S3 storage. data is the content to upload."""
        return self.client.put_object(
            bucket_name=self.name,
            object_name=name,
            length=length,
            data=data
        )

    def __get__(self, name: str) -> io.BytesIO:
        """ Get an object from S3 storage. name is the key of the object. """
        obj = self.client.get_object(
            bucket_name=self.name,
            object_name=name
        )
        data = io.BytesIO()
        data.write(obj.read())
        data.seek(0)
        return data

    def put_feather(self, df: Union[pandas.Series, pandas.DataFrame], name: str) -> str:
        """ Store a pandas.Series or pandas.Dataframe in feather format into s3 storage """
        data = io.BytesIO()
        df.to_feather(data)
        nb_bytes = data.tell()
        data.seek(0)
        return self.__put__(name, length=nb_bytes, data=data)
    
    def put_parquet(self, df: Union[pandas.Series, pandas.DataFrame], name: str) -> str:
        """ Store a pandas.Series or pandas.Dataframe in parquet format into s3 storage """
        data = io.BytesIO()
        df.to_parquet(data)
        nb_bytes = data.tell()
        data.seek(0)
        return self.__put__(name, length=nb_bytes, data=data)
    
    def put_json(
        self,
        df: Union[pandas.Series, pandas.DataFrame],
        name: str,
        **kwargs
    ) -> str:
        """ Store a pandas.Series or pandas.Dataframe in json format into s3 storage """
        data = io.BytesIO()
        json_bytes = df.to_json(path_or_buf=None, **kwargs).encode("utf-8")
        data.write(json_bytes)
        nb_bytes = data.tell()
        data.seek(0)
        return self.__put__(name, length=nb_bytes, data=data)

    def put_csv(
        self,
        df: Union[pandas.Series, pandas.DataFrame],
        name: str,
        index: bool = False,
        **kwargs
    ) -> str:
        """ Store a pandas.Series or pandas.Dataframe in csv format into s3 storage """
        data = io.BytesIO()
        csv_bytes = df.to_csv(path_or_buf=None, index=index, **kwargs).encode("utf-8")
        data.write(csv_bytes)
        nb_bytes = data.tell()
        data.seek(0)
        return self.__put__(name, length=nb_bytes, data=data)

    def put(
        self,
        df: Union[pandas.Series, pandas.DataFrame],
        name: str,
        **kwargs
    ) -> str: 
        """ 
        Store a pandas.Series or pandas.Dataframe into s3 storage.
        Format used for serialization is deduced from filename. Accepeted filenames must end with:
          - `.json`
          - `.parquet`
          - `.feather`
          - `.csv`
        """
        if name.endswith(".feather"):
            return self.put_feather(df, **kwargs)
        elif name.endswith(".parquet"):
            return self.put_parquet(df, **kwargs)
        elif name.endswith(".json"):
            return self.put_json(df, **kwargs)
        elif name.endswith(".csv"):
            return self.put_csv(df, **kwargs)

    def read_feather(self, name: str) -> Union[pandas.Series, pandas.DataFrame]:
        """ Read a pandas DataFrame stored as feather file from s3 storage. """
        data = self.__get__(name)
        return pandas.read_feather(data)

    def read_parquet(self, name: str) -> Union[pandas.Series, pandas.DataFrame]:
        """ Read a pandas DataFrame stored as parquet file from s3 storage. """
        data = self.__get__(name)
        return pandas.read_parquet(data)

    def read_json(self, name: str, **kwargs) -> Union[pandas.Series, pandas.DataFrame]:
        """ Read a pandas DataFrame stored as json file from s3 storage. """
        data = self.__get__(name)
        return pandas.read_json(data, **kwargs)

    def read_csv(self, name: str, **kwargs) -> Union[pandas.Series, pandas.DataFrame]:
        """ Read a pandas DataFrame stored as csv file from s3 storage. """
        data = self.__get__(name)
        return pandas.read_csv(data, **kwargs)
    
    def read(self, name: str, **kwargs) -> Union[pandas.Series, pandas.DataFrame]:
        """
        Read a pandas.Series or pandas.Dataframe from s3 storage.
        Format used for deserialization is deduced from filename. Accepeted filenames must end with:
          - `.json`
          - `.parquet`
          - `.feather`
          - `.csv`
        """
        if name.endswith(".feather"):
            return self.read_feather(name, **kwargs)
        elif name.endswith(".parquet"):
            return self.read_parquet(name, **kwargs)
        elif name.endswith(".json"):
            return self.read_json(name, **kwargs)
        elif name.endswith(".csv"):
            return self.read_csv(name, **kwargs)

    def listen_events(
        self,
        prefix: str = '',
        suffix: str = '',
        events: List[str] = ['s3:ObjectCreated:*', 's3:ObjectRemoved:*', 's3:ObjectAccessed:*']
    ) -> Generator[Dict, None, None]:
        """
        Return a generator that yields minio event records.
        """
        events = self.client.listen_bucket_notification(
            bucket_name = self.name,
            prefix = prefix,
            suffix = suffix,
            events = events
        )    
        for event in events:
            yield event["Records"][0]
    
    def listen_events_created(
        self,
        prefix: str = "",
        suffix: str = ""
    ) -> Generator[Dict[str, List], None, None]:
        """
        Return a generator that yields minio created event records.
        """
        for record in  self.listen_events(
            prefix = prefix,
            suffix = suffix,
            events = ['s3:ObjectCreated:*']
        ):
            yield record

    def subscribe(
        self,
        prefix: str = "",
        suffix: str = ""
    ) -> Generator[Union[pandas.DataFrame, pandas.Series], None, None]:
        """
        Subscribe to new dataframes created in bucket.
        """
        for record in self.listen_events_created(prefix=prefix, suffix=suffix):
            obj = record["s3"]["object"]
            name = obj["key"]
            yield self.read(name)
