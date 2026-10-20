# OTACast

使用 Tauri + React 的 S3 文件上传工具，支持上传进度、文件链接、按需计算 MD5 和兼容 S3 的对象存储。

## 使用流程

1. 选择文件，只读取名称和大小，不预先计算 MD5。
2. 点击「开始上传」，实时显示已传输字节数和百分比。传输结束后进行合并及远端校验，成功后展示文件 URL，支持打开和复制。
3. 上传成功后点击「计算 MD5」，展示哈希值及 `文件URL?md5=哈希值`；已有查询参数时追加 `&md5=哈希值`。

重新选文件或重新上传会清除旧结果。MD5 计算会核对本地文件大小和修改时间；如果上传后文件已变化，需要重新上传。

点击页面右上角的「运行日志」打开独立的 Sheet 侧边面板，查看本次运行的操作、上传阶段、提醒和错误。日志实时更新，最多保留最近 500 条，可手动清空；关闭面板后仍会继续记录。日志仅保存在内存中，退出软件后自动清空，不影响断点续传任务记录。

## 大文件上传与恢复

页面右上角的「未完成的上传」打开独立的 Sheet 面板，可刷新任务、继续上传、单项「放弃并清理」或「清理全部任务」。批量清理依次处理每个任务，单项失败不影响其余任务，并展示失败原因供重试。清理会删除任务记录和未完成分片，之后无法续传；已提交的远端对象会保留。关闭面板或退出软件仍保留未清理的任务，便于下次继续。

没有已记录的远端分片（没有上传 ID）的任务可直接清理，无需恢复旧配置或提供凭证。有远端分片的任务需要按面板显示的 Bucket、区域、服务地址和寻址方式，恢复 `.env` 中的 `S3_BUCKET`、`S3_REGION`、`S3_ENDPOINT`、`S3_FORCE_PATH_STYLE`，并提供有效凭证；点击「重新加载配置」后再清理。仅修改上传目录或 CDN 访问域名不会导致存储目标不匹配。

- 大于 128MiB 使用 S3 multipart upload，每片 64MiB，最多同时上传 3 片；小文件使用单次 PUT。分片范围使用 64 位字节数，覆盖 2–6GB 文件。
- 文件按 64KiB 缓冲区流式读取。每片先计算 SHA-256，再流式发送，不将整个文件或整个分片装入内存。SDK 统一处理暂时性网络错误、限流和服务端错误，最多尝试 5 次，失败时保留任务，不重传已经确认的分片。每次尝试上限 15 分钟，单次 SDK 操作总上限 90 分钟；这些不是整个文件上传的总时限。
- 「暂停并保存」保留上传记录及远端分片。退出或异常关闭后，从「未完成的上传」中继续；小文件 PUT 中断后可能需要从头发送。合并及最终校验期间不可暂停或取消。
- 恢复时核对存储目标、本地文件大小和修改时间，并重新读取已完成分片验证 SHA-256；同时通过 ListParts 核对远端分片。没有本地确认记录的远端分片会重新发送。恢复前的本地核对可能需要一些时间。
- 「取消并清理」或「放弃并清理」终止 multipart 并删除任务记录。清理失败保留 `cleanup_pending` 记录，下次启动或刷新列表时，在原存储配置可用的情况下自动重试。已经提交的对象永远不会自动删除。
- 任务记录以原子替换方式保存在系统应用数据目录下的 `uploads-v1/`，包含本地文件路径、对象目标、上传 ID 和分片摘要，不包含密钥。进程与文件锁防止同一数据目录被多个上传操作同时修改。续传可使用更新后的凭证，但 Bucket、Region、Endpoint 和寻址方式必须与原任务一致。
- 合并前再次检查本地文件状态。合并响应丢失时，使用对象中的任务标识及 checksum 查询上传结果；重启恢复时也先查询远端，避免直接重复发送。无法确认时保留记录供再次查询。

默认开启远端 SHA-256 校验：单次 PUT 核对完整文件摘要，multipart 核对由所有分片摘要计算出的组合摘要及远端大小。任务标识、ETag 和返回的版本 ID 用于确认查询的是本次对象。SHA-256 组合摘要和 ETag 都不是完整文件 MD5；「计算 MD5」仍是单独的本地按需操作，URL 中的 `md5` 参数不会触发服务器校验。

兼容服务若不支持 SHA-256，可在「上传路径与存储配置」中手动关闭后新建任务。此时明确显示「仅核对远端大小，未验证内容完整性」，不会自动降级；已有任务的校验方式保持不变。上传后的 HEAD 校验不验证 CDN 下载地址的权限和缓存配置。

新建上传统一设置对象 ACL 为 `public-read`（公共读）：任何人可读取文件，写入仍需授权；普通上传和分片上传均适用。存储服务必须启用对象 ACL 并允许公共读，否则上传会失败。此设置不会追溯修改已上传对象或已创建的分片任务。

凭证需要指定上传目录的 `s3:PutObject`、`s3:PutObjectAcl`、`s3:GetObject`、`s3:ListMultipartUploadParts` 和 `s3:AbortMultipartUpload` 权限；返回版本 ID 时还需要 `s3:GetObjectVersion`。使用 SSE-KMS 时还需按服务要求授予 KMS 权限。兼容服务的权限名称可能不同。

建议为 Bucket 配置 `AbortIncompleteMultipartUpload` 生命周期规则，例如清理 7 天未完成的分片；超过保留期的任务将无法续传。规则也用于兜底处理创建上传后响应丢失、进程崩溃或本地磁盘写入失败产生的分片。请通过存储服务控制台设置，应用不会修改桶策略。发布文件建议采用版本目录，避免覆盖同名对象。

## 验证

```bash
pnpm build
cd src-tauri
cargo test --offline
cargo clippy --offline --all-targets -- -D warnings
```

Rust 测试使用本机回环地址的模拟 S3 和虚拟凭证，覆盖分片内容及范围、单片重试、暂停后重新加载记录并恢复、合并响应丢失、校验不一致、取消清理失败与重试、小文件和空文件，以及 6GiB 的分片边界。常规测试还实际传输 0 字节、1MiB、128MiB、128MiB+1 字节文件，检查单次 PUT 与 multipart 切换、接收内容、SHA-256、进度和按需 MD5。

大文件测试需单独执行，依次生成并实际传输 2GiB、4GiB、6GiB+1 字节文件，分别检查正常上传、分片失败重试、暂停后从磁盘记录恢复。模拟服务按 64KiB 缓冲区逐字节核对接收内容，不在内存中保留大文件；每个用例结束自动删除临时文件，临时目录需要至少约 7GiB 可用空间。以下命令仅优化测试用的摘要库，避免调试构建计算摘要过慢：

```bash
cd src-tauri
cargo test --offline \
  --config 'profile.test.package.md-5.opt-level=3' \
  --config 'profile.test.package.sha2.opt-level=3' \
  gibibyte_files_transfer_retry_and_resume -- --ignored --nocapture
```

测试需要允许监听本机回环端口，不访问 `.env` 指定的真实存储。本机测试的耗时不代表公网或真实存储性能；真实服务上线前仍需使用 2GB、4GB、6GB 文件验收，包括断网、退出重开和下载校验。

真实存储验收需显式启用，默认传输 1MiB 和 129MiB。它使用应用相同的上传流程，在配置的上传目录下创建唯一的 `_otacast-tests/<任务ID>/` 对象，下载全部内容核对 MD5，然后仅删除本次对象及其版本。测试凭证额外需要 `s3:DeleteObject`，启用版本控制时还需要 `s3:DeleteObjectVersion`；清理失败会输出对象路径并保留临时记录供处理。

```bash
cd src-tauri
OTACAST_RUN_LIVE_S3=1 cargo test --offline \
  --config 'profile.test.package.md-5.opt-level=3' \
  --config 'profile.test.package.sha2.opt-level=3' \
  configured_s3_upload_download_and_cleanup -- --ignored --nocapture
```

使用 `OTACAST_LIVE_SIZES_BYTES=1048576,2147483648,4294967296,6442450945` 可指定实际测试大小（字节）。默认开启 SHA-256，兼容服务不支持时测试会明确失败；仅在明确验证应用的「仅核对远端大小」模式时设置 `OTACAST_LIVE_CHECKSUM=false`。此模式仍会通过测试程序下载全文件核对 MD5，不改变应用默认设置。

2026-10-17 的本地与真实存储测试结果见 [TEST_RESULTS.md](TEST_RESULTS.md)。

排查真实存储时，可额外设置 `OTACAST_LIVE_DIAGNOSTICS=1`，输出经过筛选的协议字段和脱敏错误链。`OTACAST_LIVE_STALL_GRACE_SECONDS=60`、`OTACAST_LIVE_MAX_ATTEMPTS=1` 仅覆盖测试客户端的停滞保护宽限期和尝试次数，用于做对照实验，不修改应用默认配置。

## 配置 S3

在项目根目录创建 `.env`：

```bash
cp .env.example .env
```

填写以下配置，值中有空格或 `#` 时请使用引号：

```dotenv
S3_BUCKET=my-bucket
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=your-access-key-id
S3_SECRET_ACCESS_KEY=your-secret-access-key

# 可选：兼容 S3 的服务地址，AWS S3 留空即可
S3_ENDPOINT=https://s3.example.com
S3_FORCE_PATH_STYLE=false

# 可选：临时凭证和默认上传目录
S3_SESSION_TOKEN=
S3_KEY_PREFIX=firmware/releases

# 可选：文件访问域名或 CDN 地址，指向 Bucket 根目录
S3_PUBLIC_BASE_URL=https://downloads.example.com

# 可选：逗号分隔的连接 IP，设置时必须同时填写 S3_ENDPOINT
S3_CONNECT_IPS=

# 可选：上传停滞宽限期，单位秒（20-300，默认 60）
S3_STALL_GRACE_SECONDS=60

# 可选：分片大小，单位 MiB（5-64）；上传并发数（1-8）
S3_PART_SIZE_MIB=64
S3_UPLOAD_CONCURRENCY=3

# 可选：是否发送校验和请求头（true 或 false，默认 true）
S3_SEND_CHECKSUM_HEADERS=true
```

`S3_BUCKET`、`S3_ACCESS_KEY_ID`、`S3_SECRET_ACCESS_KEY` 必填。`S3_REGION` 默认 `us-east-1`；`S3_FORCE_PATH_STYLE` 仅接受 `true` 或 `false`，MinIO 等服务可按需设为 `true`。默认上传目录仍可在界面调整。

文件 URL 优先使用 `S3_PUBLIC_BASE_URL` 拼接上传路径；留空时按 AWS S3 或自定义 Endpoint 的访问方式生成，并对中文、空格等路径字符编码。访问域名和 Endpoint 不应包含凭证、查询参数或片段。上传路径可在「上传路径与存储配置」中修改。新建上传使用公共读 ACL，链接的实际访问还受对象存储的公共访问限制及 CDN 配置影响。

如果厂商提供的文件访问地址是 `https://endpoint/上传路径`，不需要在域名前或路径中拼接 Bucket 名称，请设置 `S3_PUBLIC_BASE_URL=https://endpoint`。该配置直接拼接上传路径，不再自动添加 Bucket；`S3_BUCKET`、`S3_ENDPOINT` 和 `S3_FORCE_PATH_STYLE` 仍按厂商的上传接口要求配置。修改后点击「重新加载配置」，新建上传将使用新的文件访问地址。

配置由 Rust 后端读取，界面只显示 Bucket、Region 和 Endpoint，不接收密钥。`.env` 及其变体已加入 Git 忽略列表，只有 `.env.example` 提交到仓库。不要给密钥添加 `VITE_` 前缀。

## 开发

```bash
pnpm install
pnpm tauri dev
```

开发时读取项目根目录的 `.env`。修改后点击「重新加载配置」，上传时也会重新读取配置。

## 构建及分发

```bash
pnpm tauri build
```

构建会自动将项目根目录 `.env` 中支持的 S3 配置写入 Rust 应用；修改 `.env` 后再次构建会更新配置。安装包位于 `src-tauri/target/release/bundle/`，安装后的应用直接使用构建时的配置，无需再次输入。`pnpm build` 仅构建前端，生成桌面安装包请使用 `pnpm tauri build`。

如需更换已构建应用的配置，可在**实际可执行文件同目录**放置 `.env`，再点击「重新加载配置」或重启应用：Windows 放在 `.exe` 旁，macOS 放在 `.app/Contents/MacOS/`，Linux 普通程序放在可执行文件旁，AppImage 放在 `.AppImage` 文件旁。无需重新构建；不依赖启动时的工作目录。配置优先级（逐项覆盖）：**进程环境变量 > 运行时 `.env` > 构建时配置**。

也可在没有 `.env` 的情况下构建通用安装包，安装后通过上述运行时 `.env` 配置。缺少必填项时应用会显示错误并禁用上传。

**带入构建的密钥可从分发的应用中提取，请仅向可信使用者分发带有凭证的安装包。面向其他使用者分发时，建议构建不含凭证的通用包，由使用者自行提供 `.env`。**

## GitHub Actions 跨平台打包

已提供 [构建工作流](.github/workflows/build.yml)，通过 GitHub 托管的构建机生成以下安装包，无需在本地准备各系统环境：

| 构建产物名称 | 系统与架构 | 安装包 |
| --- | --- | --- |
| `otacast-windows-x64` | Windows x64 | NSIS `-setup.exe` |
| `otacast-linux-x64` | Linux x64 | `.deb`、`.rpm`、`.AppImage` |
| `otacast-macos-arm64` | macOS Apple 芯片 | `.dmg` |
| `otacast-macos-x64` | macOS Intel | `.dmg` |

### 首次配置

1. 在 GitHub 新建空仓库，不勾选初始化 README、License 或 `.gitignore`。可以使用私有仓库。
2. 将代码及工作流提交到本地 Git，再推送到 GitHub。当前项目的 `origin` 指向 Gitee，可另加名为 `github` 的远程地址，保留 Gitee 配置。将下方 `YOUR_USER` 替换为自己的 GitHub 用户名；仓库名按实际修改：

   ```bash
   git remote add github https://github.com/YOUR_USER/otacast.git
   git push github main
   ```

   推送只包含已提交的代码。当前功能修改及其依赖的新文件也需要一并提交；不要提交 `.env` 或凭证。之后可分别执行 `git push github main` 和 `git push origin main`，同步到 GitHub 与 Gitee。

3. 确认 GitHub 默认分支（通常是 `main`）已经包含 `.github/workflows/build.yml`。如仓库禁用了 Actions，在 `Settings → Actions → General` 启用，并允许此工作流引用的 Actions。

### 触发与下载

1. 进入 GitHub 仓库的 `Actions` 页面。
2. 左侧选择 `Build desktop installers`，点击 `Run workflow`，选择分支后再次点击 `Run workflow`。
3. 打开新生成的运行记录，查看四个构建任务的进度。首次构建需要下载和编译 Rust 依赖，后续运行会使用缓存。
4. 构建完成后，在本次运行的 `Artifacts` 区域下载所需系统的压缩包，解压后即可取得安装包。产物保留 14 天，下载需要登录有仓库读取权限的 GitHub 账号。

工作流仅手动触发，生成下载产物，不创建 GitHub Release。如果任务失败，打开对应任务并展开红色的步骤查看日志；其他平台会继续构建。

CI 使用固定版本的 pnpm、前端锁文件和 Cargo 锁文件构建。无需配置 S3 Secrets；工作流要求仓库根目录不存在 `.env`，生成不含 S3 凭证的通用安装包，安装后按上文配置运行时 `.env`。

当前 Windows 包未配置代码签名；macOS 采用不需要证书的 ad-hoc 签名，未公证，下载后仍可能被系统安全机制拦截。正式对外发布前需另外配置开发者证书及 macOS 公证。

参考：[Tauri GitHub Actions](https://v2.tauri.app/distribute/pipelines/github/)、[GitHub 手动运行工作流](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow)、[下载构建产物](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/download-workflow-artifacts)。
