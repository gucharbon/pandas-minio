#/usr/bin/bash


#ACCESS_KEY=${ACCESS_KEY:-"access_key"}
#SECRET_KEY=${SECRET_KEY:-"secret_key"}

echo "access_key" | docker secret create minio_access_key -
echo "secret_key" | docker secret create minio_secret_key -
