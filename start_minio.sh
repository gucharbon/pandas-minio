#/bin/bash

docker service create --name="minio-service" \
  --secret="minio_access_key" \
  --secret="minio_secret_key" \
  --env="MINIO_ACCESS_KEY_FILE=minio_access_key" \
  --env="MINIO_SECRET_KEY_FILE=minio_secret_key" \
  -p 9001:9000 \
  minio/minio server /data
