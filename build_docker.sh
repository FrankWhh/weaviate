# 1. 注册多架构模拟器（只需执行一次）
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

# 输出应包含：
# Setting up qemu-binfmt-conf...
# Updating binfmt.d for qemu-user-static

# 2. 验证注册成功
ls -la /proc/sys/fs/binfmt_misc/ | grep qemu
# 应看到: qemu-aarch64, qemu-x86_64 等

# 3. 确保 buildx builder 已启用平台支持
docker buildx inspect --bootstrap
# 检查输出中的 "Platforms" 是否包含 linux/arm64

VERSION="1.36.2.5"

# 构建 amd64
docker buildx build \
  --platform linux/amd64 \
  --build-arg GIT_REVISION="$(git rev-parse HEAD)" \
  -t "semitechnologies/weaviate:${VERSION}" \
  --load \
  .

# 构建 arm64
docker buildx build \
  --platform linux/arm64 \
  --build-arg CGO_ENABLED=0 \
  --build-arg GIT_REVISION="$(git rev-parse HEAD)" \
  -t "semitechnologies/weaviate:${VERSION}-arm64" \
  --load \
  .

# 查看结果
docker images | grep weaviate
