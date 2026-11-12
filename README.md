# Multi-Center DICOM Ingest API

多中心 DICOM 入库平台。解决两个核心问题：

1. **设备在不同中心复用同一个 DICOM UID** 时，分割/引用结果可能错误地指向
   别家中心的影像。本平台对每个来源中心独立、确定性地重映射所有实例标识，使
   跨中心数据在物理上无法串联。
2. **各中心 `BodyPartExamined` 写法不一致**，无法按统一部位筛选研究队列。
   平台提供可整体替换的部位索引：接收“中心 + 原值 → 标准部位”的完整字典，
   精确匹配，未收录或空值统一记为 `UNCLASSIFIED`；后台按新字典重建 SQLite
   索引，期间照常入库、查询仍走旧字典，切换时字典与全部索引在一个事务内
   原子生效。

技术栈：**FastAPI + pydicom + SQLite（WAL 模式）**，单容器即可运行，交付
Docker Compose 环境与交互式 API 文档（Swagger UI）。

---

## 1. 核心保证

| 需求 | 实现方式 |
| --- | --- |
| 按“中心 + 原 UID”稳定生成新 UID | `new_uid = "2.25." + uuid5(固定命名空间, center + "|" + 原UID).int`，纯函数，无需查库、跨进程/重启不变（`app/uid.py`） |
| 跨中心同原 UID 不碰撞 | 中心标识参与哈希输入；同原 UID 在不同中心必然得到不同新 UID，引用永不跨中心 |
| 统一改写范围 | 顶层 Study/Series/SOP Instance UID、文件头 `MediaStorageSOPInstanceUID`、任意序列深度嵌套的 `ReferencedSOPInstanceUID`、序列内 `SeriesInstanceUID`、`FrameOfReferenceUID`、`ReferencedFrameOfReferenceUID`（递归遍历，`app/dicom_io.py`） |
| 不改写的内容 | SOP Class UID / Transfer Syntax UID 等“类别型”UID、Modality 等属性、**PixelData 像素字节**（写入后回读断言验证） |
| 仅接受 Explicit VR Little Endian | 校验文件头传输语法必须为 `1.2.840.10008.1.2.1`，否则整批 422 拒绝 |
| 引用先到、影像后到 | 引用先入库时在 `uid_mappings` 预留映射并在 `instance_references` 标记未解析；后续同中心影像到达后批量重新解析；不同中心到达的影像不会解析它 |
| 同中心同原 SOP 同字节重传 | 命中 `(center, original_sop_uid, sha256)` 唯一约束，直接复用已存对象，不产生重复行/重复文件 |
| 异字节冲突 | 同中心同原 SOP UID 但 SHA-256 不同 → 整批 409 拒绝 |
| 任一文件失败整批不入库 | 全部文件先在事务外解析；入库在单个 `BEGIN IMMEDIATE` 事务内完成，失败回滚，并清理已落盘的临时对象，**不存在半批可下载** |
| 并发安全 | SQLite WAL + 写事务互斥；唯一约束兜底；12 路并发实测全部 201 |
| 重启不变 | 映射为纯函数可离线复算，数据库与对象文件落盘到挂载卷 |
| 批量上限 | 每批最多 20 份文件（可配置） |
| 部位字典精确匹配 | 完整“中心 + 原值 → 标准部位”字典，按 `(中心, 原值)` 精确（区分大小写）匹配；空值、缺属性或未收录原值一律归入 `UNCLASSIFIED`（`app/bodypart.py`） |
| 在线重建、不阻塞入库 | 新字典触发后台线程重建：读取 DICOM 文件时**不持有正式库写事务**，暂存行通过独立短事务写入；重建期间入库与查询继续，查询仍用旧字典 |
| 原子切换、单一口径 | 切换在单个 `BEGIN IMMEDIATE` 事务内完成整索引替换 + 字典指针翻转；期间已成功入库的实例由切换前回填循环纳入；任何单次查询只见一种字典口径 |
| 新实例与索引共同提交 | 入库事务内同步写入当前活动字典的索引行（`INSERT OR IGNORE`）；重传复用既有实例、不产生重复索引行 |
| 读取失败即替换失败 | 任一对象无法读取时本次替换标记 `failed`、清理暂存行、**旧索引完整保留**，错误信息包含出错实例 ID/中心/路径与原因 |
| 重启安全 | 启动时把残留 `building` 重建标记为 `failed`；旧活动索引与已确认入库不受影响 |
| 同时仅一次替换 | 进程锁 + 数据库部分唯一索引（`WHERE status='building'`）双重保证，重复提交返回 409 |
| 首次就绪前查询提示 | 尚无活动字典时，部位查询与活动字典接口返回 503 `not ready` |

---

## 2. 目录结构

```
app/
  config.py        环境变量配置（数据目录、UID 根、批量上限等）
  uid.py           确定性 UID 重映射（uuid5 + 2.25 根）
  dicom_io.py      DICOM 解析/校验、递归 UID 改写、序列化后回读校验、
                   BodyPartExamined 提取与磁盘对象部位读取
  db.py            SQLite schema、WAL 连接、即时写事务
  service.py       批次原子入库、重传复用、字节冲突、引用解析、新实例入索引
  bodypart.py      部位字典校验、后台重建、原子切换、重启恢复、队列查询
  main.py          FastAPI 路由与响应模型
scripts/
  generate_samples.py  生成合成样例（跨中心碰撞 / 引用先到 / 非法文件 / 部位写法差异）
  demo.sh              对运行中的服务执行完整 HTTP 演示
tests/             36 项 pytest（含并发、原子性、嵌套改写、重启、字典重建与切换）
samples/generated/ 合成 DICOM 样例（构建时生成）
Dockerfile, docker-compose.yml, docker/entrypoint.sh
```

---

## 3. 快速开始（Docker Compose）

```bash
# 1. 生成合成样例（需要本机有 pydicom；也可直接用仓库内已生成的文件）
python3 scripts/generate_samples.py

# 2. 构建并启动
docker compose up --build -d

# 3. 健康检查
curl http://localhost:8000/healthz
# {"status":"ok"}

# 4. 打开交互式 API 文档
#    浏览器访问 http://localhost:8000/docs
```

在 Swagger UI 中演示上传：

1. 展开 `POST /api/v1/batches` → **Try it out**
2. `center_id` 填 `center-a`
3. `files` 选择 `samples/generated/cross_center_uid_collision/center-a/image_001.dcm`
4. **Execute**，响应 201，返回每个文件的原/新 UID 映射
5. 用 `GET /api/v1/batches/{batch_id}` 查询批次，用
   `GET /api/v1/instances/{id}/download` 下载改写后的 DICOM

部位字典替换、队列筛选与下载（Swagger 的 **body-part** 分组）：

1. 先调 `GET /api/v1/body-part/instances`（任意中心/部位）确认返回 503
   `not ready`
2. 上传 `samples/generated/bp-dict/center-{a,b,c}/` 下的对象
3. 展开 `POST /api/v1/body-part/dictionaries` → **Try it out**，粘贴
   `scripts/demo.sh` 中 `dict-v1.json` 的内容并 **Execute**，响应 202
4. 用 `GET /api/v1/body-part/rebuilds/{id}` 轮询到 `active`，再用
   `GET /api/v1/body-part/instances?center_id=center-b&standard_body_part=CHEST`
   验证不同写法被统一归类，响应中的 `download_url` 可直接下载
5. 再提交 `dict-v2.json`（`CHEST` 队列改名 `THORAX`），确认切换后旧标签
   查询为空、新标签生效，`GET /api/v1/body-part/rebuilds` 可见
   `superseded`/`active` 历史

### 一键脚本演示（覆盖全部需求场景）

```bash
BASE_URL=http://localhost:8000 bash scripts/demo.sh
```

脚本依次演示：跨中心 UID 碰撞、引用先到、跨中心不解析、同中心影像到达后解析、
同字节重传复用、异字节整批 409、批次查询与下载校验像素不变、非法文件整批 422。

### 不用 Docker 的本地运行

```bash
pip install -r requirements.txt
python3 scripts/generate_samples.py
DICOM_INGEST_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 4. HTTP API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/batches` | multipart 表单：`center_id` + `files`（1–20），原子入库 |
| GET | `/api/v1/batches` | 批次列表，可按 `center_id` 过滤 |
| GET | `/api/v1/batches/{batch_id}` | 批次详情 + 每个文件的原/新 UID 映射 |
| GET | `/api/v1/instances` | 实例检索（`center_id` / `original_sop_uid` / `new_sop_uid`） |
| GET | `/api/v1/instances/{id}/download` | 下载改写后的 DICOM（`application/dicom`） |
| GET | `/api/v1/references/pending` | 某中心的引用解析状态（`resolved=true/false`） |
| POST | `/api/v1/body-part/dictionaries` | 提交完整部位字典，返回 202 并启动后台重建 |
| GET | `/api/v1/body-part/rebuilds` | 重建/替换历史（`building/active/superseded/failed`） |
| GET | `/api/v1/body-part/rebuilds/{id}` | 单次替换状态；失败时含出错实例与原因 |
| GET | `/api/v1/body-part/active` | 当前生效字典与已索引实例数（无字典时 503） |
| GET | `/api/v1/body-part/instances` | 按 `center_id` + `standard_body_part` 查实例 ID 与下载地址 |
| GET | `/healthz` | 存活探针 |

错误语义：`422` = 文件/参数不合法（含非法 DICOM、传输语法不符、超 20 份、
字典条目重复/空值）；`409` = 与既有状态冲突（异字节同 UID、已有重建进行中、
版本号重复）；`404` = 批次/实例/重建不存在；`503` = 首个部位字典尚未激活，
索引未就绪。

---

## 5. 合成样例说明

`scripts/generate_samples.py` 生成的全部对象均为带 DICM 前导、文件头和真实
PixelData 的合法 Explicit VR Little Endian 文件：

- `cross_center_uid_collision/center-{a,b}/image_001.dcm`
  两个中心**复用完全相同的 Study/Series/SOP UID**，但像素值分别为 1000 与
  2000（异字节）。入库后得到两个不同的新 SOP UID，像素各自保持不变。
- `reference_first/struct_001.dcm`
  SR 对象，含三层嵌套
  （`ReferencedSOPSequence → ReferencedSeriesSequence → ReferencedSOPSequence`），
  引用一个尚未上传的影像 SOP。先上传它会产生一条未解析引用。
- `reference_first/image_001.dcm`
  被引用的影像。同中心上传后引用自动解析；别的中心上传同原 UID 影像不会解析。
- `invalid/not_a_dicom.dcm`
  非 DICOM 字节，用于演示整批拒绝。
- `bp-dict/center-{a,b,c}/*.dcm`
  同一解剖区域在不同中心的不同 `BodyPartExamined` 写法（如 `CHEST` /
  `XIONG` / 小写 `chest`），另有空值对象与完全不带该属性的对象，用于演示
  精确匹配、大小写敏感与 `UNCLASSIFIED` 归类。

---

## 5b. 部位字典与索引：在线替换机制

### 提交字典

`POST /api/v1/body-part/dictionaries` 的请求体是**完整字典**（不是增量补丁）：

```json
{
  "version_label": "bodypart-v1",
  "entries": [
    {"center_id": "center-a", "original_value": "CHEST", "standard_body_part": "CHEST"},
    {"center_id": "center-b", "original_value": "XIONG", "standard_body_part": "CHEST"},
    {"center_id": "center-c", "original_value": "chest", "standard_body_part": "CHEST"}
  ]
}
```

* 匹配按 `(center_id, original_value)` **精确、区分大小写**进行；
* 空字符串/纯空白的 `original_value` 在接口侧拒绝——空值本就自动归入
  `UNCLASSIFIED`，无需也不允许显式配置；
* 同一对 `(center_id, original_value)` 重复出现返回 422；
* 响应 `202 Accepted`，返回重建任务 ID，后台立即开始重建。

### 重建与切换的状态机

```
POST 字典 ──► rebuilds 行 building（部分唯一索引：全局至多一行）
                 │  后台线程：逐个读取已存储 DICOM（不持写事务）
                 │  分类结果写入 body_part_index_staging（短事务）
                 │  回填循环：补齐扫描期间新入库的实例
                 ▼
        单个 BEGIN IMMEDIATE 事务：
          校验暂存覆盖全部实例 → 清空旧索引 → 写入新索引
          → 旧 active 置 superseded → 本次置 active → 翻转指针
                 │                              │
            任一 DICOM 读取失败                提交完成
                 ▼                              ▼
          failed：删暂存、记录出错实例      后续查询全部走新字典
          与原因，旧索引原样服务
```

* **期间继续入库**：入库走自己的写事务，并在同一事务内按**当前活动字典**
  写索引行；重建线程的 DICOM 解析不占用写锁，二者互不阻塞。
* **切换纳入期间入库**：切换事务先在写锁下复查暂存缺口，若扫描后又有新
  实例则本次不切换、回到回填循环，直到暂存集合与实例集合严格相等才提交。
* **单次查询单一口径**：查询通过活动指针关联单一 `dict_id` 的索引；切换
  提交前的查询只见旧字典，提交后的查询只见新字典，不存在混合结果。
* **读取失败**：出错实例的 ID、中心、存储路径与异常原因写入
  `error_detail`，重建 `failed`，暂存清理，旧索引与已确认入库完整保留；
  修复后可用新版本号重新提交。
* **重启**：启动生命周期钩子把残留 `building` 行全部置 `failed`（原因
  注明重启中断），活动索引不变。

### 队列查询

```bash
curl "$BASE/api/v1/body-part/instances?center_id=center-b&standard_body_part=CHEST"
```

返回实例 ID、新 SOP UID、原值、标准部位、所属字典 ID 和可直接使用的
`download_url`；查空值/未收录对象时 `standard_body_part=UNCLASSIFIED`。
首个字典激活前该接口与 `/active` 返回 503。

---

## 6. 数据模型要点

- `uid_mappings`：每个 `(center, 原UID)` 一行，`present` 标识携带该 SOP 的实体
  是否已到达，支撑引用先到。
- `instances`：每个入库对象一行，含原/新三套 UID、SHA-256、存储相对路径，
  `(center, original_sop_uid, sha256)` 与 `new_sop_uid` 均唯一。
- `instance_references`：引用关系与解析状态，按中心隔离。
- `batches`：仅记录成功提交的批次；被拒绝的批次不留任何记录或对象。
- `body_part_dicts`：每个字典版本一行，完整映射以 JSON 文档整体存储，
  保证字典永远作为一个整体与对应索引一起生效。
- `rebuilds`：每次替换一行，状态机 `building → active / failed`，旧活动行
  切换后变 `superseded`；部分唯一索引保证任意时刻至多一个 `building`。
- `body_part_index`：**仅存当前活动字典**的索引（实例 ID、中心、原值、
  标准部位、字典 ID）。
- `body_part_index_staging`：重建暂存区，按 `rebuild_id` 隔离；失败可整体
  清理，切换后删除。
- `app_state`：单行键值表，保存活动重建/字典指针，跨重启有效。

所有表在容器启动时通过 `CREATE TABLE IF NOT EXISTS` 幂等建表（对既有库为
增量迁移，不影响已有数据）。

---

## 7. 运行测试

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q
# 36 passed
```

测试覆盖：UID 纯函数性质、跨中心碰撞隔离、嵌套序列改写、像素/传输语法保持、
引用先到后解析与跨中心不解析、同字节重传复用、异字节/非法文件整批回滚、
20 份上限、非 Explicit VR 拒绝、8 路并发与 6 路并发重传收敛；以及部位索引的
首次未就绪 503、精确匹配与大小写敏感、`UNCLASSIFIED` 归类、重建与原子切换、
重建期间入库被纳入、重传无重复索引行、单次替换互斥（409）、DICOM 读取失败
保留旧索引并上报出错实例、重启后中断替换标失败且旧索引保持、字典载荷校验。

---

## 8. 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DICOM_INGEST_DATA_DIR` | `/data` | SQLite 库与对象文件根目录（Compose 已挂卷） |
| `DICOM_INGEST_MAX_FILES` | `20` | 单批文件数上限 |
| `DICOM_INGEST_MAX_FILE_BYTES` | `52428800` | 单文件大小上限 |
| `DICOM_INGEST_UID_ROOT` | `2.25` | 新 UID 根（2.25 = UUID 派生 UID 的 ISO/ITU 保留根） |
| `DICOM_INGEST_BUSY_TIMEOUT_MS` | `30000` | SQLite 写锁等待超时 |
