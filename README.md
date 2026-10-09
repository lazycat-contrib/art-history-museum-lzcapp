# art-history-museum-lzcapp

[A Walkable History of Art](https://github.com/justdataplease/art-history-museum)（艺术史博物馆）的懒猫微服打包。

一个可在浏览器中漫步的 3D 博物馆：跟随时间线，选择画家，走进他们的画廊——572 位画家、约 10 万件作品，数据来自 Wikipedia / Wikidata / Wikimedia Commons。

## 交付方式

上游**没有**发布现成的容器镜像，因此本仓库负责构建：

1. [`build-image.yml`](.github/workflows/build-image.yml) — 从上游源码构建镜像并推送到
   `ghcr.io/lazycat-contrib/art-history-museum`（tag 为 `v<package.yml 版本>`），
   每日定时检查上游新提交，也可以手动触发。
2. [`lazycat.yml`](.github/workflows/lazycat.yml) — 通过
   [lazycat-github-action](https://github.com/ca-x/lazycat-github-action) 打包 LPK
   并发布到喵喵（私有）商店（镜像经 `ghcr.1ms.run` 加速交付）。

### 更新流程

1. 确认上游有新提交（`build-image.yml` 的定时任务会自动构建 `latest`；如需发新版本）
2. 修改 `package.yml` 的版本号并提交
3. 手动触发 `build-image.yml`（会构建并推送 `v<新版本>` 镜像）
4. 手动触发 `lazycat.yml`，或等下次定时任务，把新版本发布到商店

## 运行说明

- 容器端口 3000，懒猫网关负责 TLS，应用内部直接以明文 HTTP 运行
- 应用整站公开（`public_path: /`），无需懒猫账号即可漫步博物馆
- 数据（约 152 MB 的 museum.json）已内嵌在镜像中，无需额外数据库

## 免责声明

应用内容（画家资料与画作图像）来自 Wikipedia、Wikidata 与 Wikimedia Commons，
遵循其各自的许可协议；本项目仅为打包，不修改上游内容。上游项目许可证：MIT。
