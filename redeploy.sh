docker buildx build \
  --platform linux/arm64,linux/amd64 \
  -t eddisonso/gfs-master:latest \
  --push .

docker buildx build \
  --platform linux/arm64,linux/amd64 \
  -t eddisonso/gfs-chunkserver:latest \
  --push .

kubectl rollout restart deployment/gfs-master

kubectl rollout restart daemonset/gfs-chunkserver
