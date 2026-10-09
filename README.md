# art-history-museum-lzcapp

[A Walkable History of Art](https://github.com/justdataplease/art-history-museum)（艺术史博物馆）的懒猫微服打包。

一个可在浏览器中漫步的 3D 博物馆：跟随时间线，选择画家，走进他们的画廊——572 位画家、约 10 万件作品，数据来自 Wikipedia / Wikidata / Wikimedia Commons。

## 交付方式（镜像从源码构建并内嵌，无 registry 依赖）

上游**没有**发布现成的容器镜像，本仓库在 CI 里从 vendored 的 `upstream/` 源码自行构建，
全部交给 [lazycat-github-action](https://github.com/ca-x/lazycat-github-action) v1.3.0：

1. [`lazycat.yml`](.github/workflows/lazycat.yml) 调用 action 的 `auto` 流程：在 GitHub
   runner 上用 docker buildx 构建 [`lzc-build.yml`](lzc-build.yml) 里 `images:` 定义的镜像，
   并以**完整 OCI 格式直接嵌入 LPK**（[`lzc-manifest.yml`](lzc-manifest.yml) 里
   `image: embed:...`）——不推送任何镜像仓库，因此无需 GHCR 及镜像可见性配置。
2. 构建产物（`<package-id>-v<version>.lpk`）发布为 GitHub Release 资产，
   同时上架喵喵（私有）商店。

> 此前用 `lzc-cli` 在 CI 里本地打包（并因 2.0.9+ 的 layer-tar 截断缺陷锁定 2.0.8），
> action v1.3.0 内置同等的源码镜像内嵌能力后已完全替代。

### 更新流程

1. 同步 `upstream/` 到上游新提交（vendored 源码，直接提交进本仓库）
2. 修改 `package.yml` 的版本号并提交
3. 手动触发 `lazycat.yml`：构建镜像 → 生成 LPK → 发布 Release 资产 → 上架商店

## 运行说明

- 容器端口 3000，懒猫网关负责 TLS，应用内部直接以明文 HTTP 运行
- 应用整站公开（`public_path: /`），无需懒猫账号即可漫步博物馆
- 数据（约 152 MB 的 museum.json）已内嵌在镜像中，无需额外数据库

## 免责声明

应用内容（画家资料与画作图像）来自 Wikipedia、Wikidata 与 Wikimedia Commons，
遵循其各自的许可协议；本项目仅为打包，不修改上游内容。上游项目许可证：MIT。
