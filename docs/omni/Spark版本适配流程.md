# Gluten/OmniOperator Spark 版本适配流程

本文说明 Gluten/OmniOperator 适配指定 Spark 版本时的判断、修改、编译和验证流程。

## 1. 适用范围和前提

本文以“替换”为默认适配方式：同一个 Spark `major.minor` 版本线只选择一个 `patch` 版本作为主版本，不同时维护多个 patch 版本的独立产物。

例如，Gluten 已有 Spark 3.2 profile，原主版本是 Spark 3.2.2，现在需要适配 Spark 3.2.0，则直接把 Spark 3.2 profile 的主版本由 3.2.2 替换为 3.2.0，并按 Spark 3.2.0 的 API 和运行时行为修改 `spark32` shim 及公共代码。

替换完成后：

- Spark 3.2.0 是 `spark-3.2` profile 的编译和完整验证目标。
- Spark 3.2.2 不再作为该 profile 的承诺支持版本，也不要求继续生成独立产物。
- Spark 3.3、3.4、3.5 等其他仍受支持的版本线需要做回归编译，防止公共代码修改破坏它们。

如果产品要求同时正式支持 Spark 3.2.0 和 3.2.2，则不属于本文的替换模式，需要另行设计独立 profile、shim、产物命名和测试矩阵。

## 2. Spark 版本号说明

以 Spark `3.2.0` 为例：

| 位置 | 名称 | 中文含义 | 示例含义 |
| --- | --- | --- | --- |
| `3` | major version | 主版本号 | Spark 3 |
| `2` | minor version | 次版本号 | Spark 3 系列中的 3.2 版本线 |
| `0` | patch version | 补丁版本号 | Spark 3.2 版本线中的第 0 个补丁版本 |

本文中的“已有对应 minor”是指已经存在同一 `major.minor` 的 profile 和 shim。例如目标是 Spark 3.2.0，而仓库已有 `spark-3.2` profile 和 `shims/spark32`，就属于已有对应 minor；即使 profile 当前使用的是 Spark 3.2.2，也不需要新建 `spark30` 或另一套并行 shim。

日常沟通中常把 Spark 3.2 称为“大版本”，但在本文中统一称为“3.2 版本线”或“minor 版本”，避免和 Spark 3、Spark 4 这样的 major version 混淆。

## 3. 适配前确认

开始修改前，必须确认以下信息：

1. 目标 Spark 完整版本，例如 `3.2.0`，不能只写 `3.2`。
2. 当前 profile 的主 Spark 版本，例如 `3.2.2`。
3. 本次采用替换模式，以目标版本覆盖当前主版本。
4. 目标运行环境使用的 Scala、Hadoop 和 JDK 版本。
5. 目标环境中的 `SPARK_HOME` 确实指向目标 Spark 完整版本。
6. 先保存当前主版本的编译结果和最小运行测试结果，作为适配基线。

除 `spark.version` 外，还应检查 Spark 关联依赖，包括：

- `sparkbundle.version`
- `sparkshim.artifactId`
- `scala.version` 和 `scala.binary.version`
- `hadoop.version`
- `iceberg.version`
- `delta.package.name`、`delta.version` 和 `delta.binary.version`
- `hudi.version`
- `antlr4.version` 等随 Spark 版本调整的依赖

## 4. 判断现有支持情况

首先检查 `Gluten/pom.xml` 中是否存在目标版本线的 Maven profile，并检查 `Gluten/shims/` 下是否存在对应 shim。

例如 Spark 3.2 对应：

```text
Gluten/pom.xml                         profile: spark-3.2
Gluten/shims/spark32/                 Spark 3.2 专属 shim
Gluten/shims/pom.xml                  spark32 module 注册
```

根据检查结果进入以下分支。

### 4.1 已有完全一致的 Spark 版本

如果 `spark-3.2` profile 的 `spark.version` 已经是 `3.2.0`，并且 `shims/spark32` 已存在，则先不修改版本配置，直接用该 profile 编译和运行验证。

如果编译或运行失败，再根据错误处理 shim、公共代码或后端代码的兼容问题。

### 4.2 已有对应 minor，但 patch 版本不同

这是本文的主要场景。例如目标是 Spark 3.2.0，仓库已有 Spark 3.2 profile 和 shim，但当前主版本为 Spark 3.2.2。

处理方式是：

1. 保留 `spark-3.2` profile 和 `shims/spark32` 目录。
2. 把 `spark-3.2` profile 的 `spark.version` 从 `3.2.2` 替换为 `3.2.0`。
3. 复核该 profile 中的 Spark 关联依赖版本。
4. 把 `SparkShimProvider.DESCRIPTOR` 的完整测试版本同步为 `3.2.0`。
5. 使用 Spark 3.2.0 的真实 API 编译并修改 `spark32` shim。
6. 在真实 Spark 3.2.0 运行时完成验证。

不能因为 3.2.0 和 3.2.2 属于同一 minor 就认为二进制完全兼容。Gluten 会使用较多 Spark internal API，patch 版本之间仍可能出现方法签名、可见性、构造器和类 ABI 差异。

### 4.3 没有对应 minor

这种情况较少。例如目标是 Spark 3.6.x，但仓库只有 `spark-3.5` profile 和 `shims/spark35`。

可以复制最接近的 shim 作为起点，但不能只复制目录。至少需要完成：

1. 复制 `Gluten/shims/spark35` 为 `Gluten/shims/spark36`。
2. 修改目录内的 package、类名、artifactId 和 `SparkShimDescriptor`。
3. 在 `Gluten/pom.xml` 新增 `spark-3.6` profile，配置完整 `spark.version`、`sparkbundle.version`、`sparkshim.artifactId` 和关联依赖。
4. 在 `Gluten/shims/pom.xml` 的 `spark-3.6` profile 中注册 `spark36` module。
5. 更新 `META-INF/services/org.apache.gluten.sql.shims.SparkShimProvider`。
6. 更新统一构建脚本支持的版本列表、帮助信息和 bundle jar 匹配规则。
7. 对照目标 Spark 源码和实际 jar 修改 shim，不能默认 Spark 3.5 的 API 可直接用于 Spark 3.6。

## 5. 修改原则

编译错误出现后，应先判断差异属于哪一层，再选择修改位置。

### 5.1 优先放入 Spark shim 的差异

下列差异优先通过 `SparkShims` 接口和版本专属实现隔离：

- 同一能力在不同 Spark 版本中的方法名或参数不同。
- 构造器、返回类型或 extractor 参数数量不同。
- Spark internal API 的可见性发生变化。
- 表达式、执行计划或 writer API 只存在于部分 Spark 版本。

公共代码调用 `SparkShimLoader.getSparkShims`，各版本 shim 分别完成具体实现，避免在 `backends-omni` 中散落版本判断。

### 5.2 可以放在公共代码中的兼容处理

以下情况可以在公共代码中处理：

- 可以通过稳定的 class name 判断可选类型，避免编译期直接引用目标版本不存在的类。
- 能力在旧版本完全不存在，可以通过 capability 判断禁用或回退相关优化。
- 调整后仍能保持所有受支持 Spark 版本的相同行为和二进制接口。

### 5.3 需要特别检查的同包同名类

`shims` 和 `backends-omni` 可能复制 Spark 的同包同名 internal class。Maven shade 遇到重复类时可能只保留其中一份，即使编译成功，也可能在运行时出现 `NoSuchMethodError`、`NoSuchFieldError` 或 `ClassCastException`。

因此应优先消除重复定义；确实无法消除时，最终进入 bundle 的类必须兼容所有实际调用方需要的构造器、字段 accessor 和方法 ABI。

Spark 3.2.0 适配中出现的 `WriteJobDescription.bucketIdExpression()` 问题就是典型案例：编译和打包成功，但同包同名类的 ABI 不一致导致运行时失败。

## 6. 通过脚本编译

适配过程中统一通过构建脚本编译，根据真实编译错误修改代码，再提交、推送并重新执行脚本，直到完整构建通过。

### 6.1 Windows 提交与 Linux 编译边界

本地 Windows 仓库有修改时，先在 Windows 完成检查、commit 和 push。Linux 服务器不接收本地额外上传的源码、patch 或脚本；服务器编译脚本通过 `--fetch_and_reset` 自动 fetch 并 reset 到远端当前分支。

本次 Spark 3.2.0 适配的固定顺序是：

1. 在 Windows 的 Gluten 和 OmniOperator 仓库分别检查工作区，只提交本次适配相关文件。
2. 将两个仓库的 commit push 到各自远端当前分支。
3. SSH 到 Linux 编译目录，使用统一构建脚本并带上 `--fetch_and_reset`。
4. 不执行额外的 `scp`、源码目录上传或临时脚本上传。

`--fetch_and_reset` 会在构建前对服务器上的 `Gluten` 和 `OmniOperator` 执行 `git fetch origin` 以及针对当前分支的 `git reset --hard origin/<branch>`。因此 Windows push 的分支必须与服务器当前工作区分支和远端跟踪关系一致。

本节后续命令在 Linux 宿主机执行，统一使用 `/opt/omni-operator-build` 作为编译工作目录。

#### 准备编译工作目录

```bash
mkdir -p /opt/omni-operator-build
cp -r /opt/omni-operator \
  /opt/omni-operator-build/
cd /opt/omni-operator-build
```

首次准备时，将已有的 `/opt/omni-operator` 复制到编译工作目录，作为构建使用的 `OMNI_HOME`。

#### 获取构建脚本

`build_omnioperator.sh` 从 [OmniOperator 2.2.0 发布包 OmniOperator.zip](https://gitcode.com/openeuler/OmniOperator/releases/download/26.1.0-OmniOperator-2.2.0-internet-release/OmniOperator.zip) 中获取。在 Linux 宿主机下载并解压发布包，将其中的 `build_omnioperator.sh` 放到 `/opt/omni-operator-build/`，后续通过该脚本执行编译。

#### 首次获取源码

```bash
cd /opt/omni-operator-build
git clone https://gitcode.com/openeuler/Gluten.git -b 2026_930_poc
git clone https://gitcode.com/openeuler/OmniOperator.git -b 2026_930_poc
```

已有仓库时跳过 clone，后续由构建脚本的 `--fetch_and_reset` 更新。Windows 提交必须推送到 Linux 仓库 `origin` 指向的远端仓库和同一分支，确保服务器能够获取本次修改。

#### 准备 Docker 容器

```bash
docker pull swr.cn-north-4.myhuaweicloud.com/cloud_boostkit/boostkit_omni:omni_2203_001
docker run -dit --name compile_omnioperator --privileged \
  -v /opt/omni-operator-build:/opt/omni-operator-build:rw \
  swr.cn-north-4.myhuaweicloud.com/cloud_boostkit/boostkit_omni:omni_2203_001 /bin/bash
docker ps --filter name=compile_omnioperator
```

首次创建容器时执行 `docker run`；已有同名容器时复用该容器，若已停止则执行 `docker start compile_omnioperator`。

仅挂载编译工作目录，不要挂载整个 `/opt`，以免覆盖镜像内的 `/opt/buildtools` 编译依赖。`/opt/omni-operator-build/omni-operator` 位于挂载目录内，宿主机和容器使用相同路径，无需额外挂载。

确认统一构建脚本 `build_omnioperator.sh` 已在 `/opt/omni-operator-build` 中，并检查仓库分支和工作区状态后，按第 6.2 节执行编译。`--fetch_and_reset` 会丢弃服务器仓库中的已跟踪文件本地修改，执行前应确认这些修改不需要保留。

### 6.2 使用仓库统一构建脚本

在 Linux 宿主机的编译工作目录执行：

```bash
cd /opt/omni-operator-build
chmod 750 ./build_omnioperator.sh
bash build_omnioperator.sh --docker compile_omnioperator \
  --omni_home /opt/omni-operator-build/omni-operator \
  --fetch_and_reset
```

使用本文 Spark 3.2 构建入口时，应从编译日志确认实际启用 `spark-3.2` profile，依赖为 Spark 3.2.0；`--omni_home` 仅指定产物目录，目录名称不能作为实际 Spark 版本的依据。

每轮编译错误都应按第 5 章分类处理，避免为了消除单个错误把高版本代码直接复制到公共模块。

非交互 SSH、`nohup` 或后台 Docker 构建需要 pseudo-TTY 时，使用统一脚本入口：

```bash
script -q -c \
  'bash build_omnioperator.sh --docker compile_omnioperator --omni_home /opt/omni-operator-build/omni-operator --fetch_and_reset' \
  /dev/null
```

## 7. 打包检查

完整构建成功后，不能只检查 `BUILD SUCCESS`，还应检查：

1. bundle jar 名称包含正确的 Spark 版本线，例如 `spark3.2`。
2. bundle jar 和 native SO 已复制到本次使用的 `OMNI_HOME/lib/`。
3. 产物时间戳来自本次构建。
4. shade 日志中是否存在 Spark internal class 重复。
5. bundle 中是否同时出现多个版本的 shim provider 或不应进入的 class。
6. 运行节点上的旧 jar 已被替换，driver 和 executor 使用同一套产物。

重点关注 shade warning 中来自以下模块的同包同名类：

```text
spark-sql-columnar-shims-spark32
backends-omni
Spark 自身依赖
```

重复类 warning 不能一律忽略。只要重复类包含 Spark internal API，就需要确认最终保留的类及其 ABI。

## 8. 添加 Spark 参数与启动验证

复制 Spark 配置，避免修改公共配置：

```bash
export SPARK_HOME=/usr/local/spark
cp "$SPARK_HOME/conf/spark-defaults.conf" \
   "$SPARK_HOME/conf/spark-defaults-omni.conf"
chmod 640 "$SPARK_HOME/conf/spark-defaults-omni.conf"
```

将以下内容追加到 `spark-defaults-omni.conf`。

### 8.1 参数配置

```properties
spark.memory.offHeap.enabled true
spark.memory.offHeap.size 7g
spark.executor.memory 2g
spark.executor.extraJavaOptions -XX:MaxDirectMemorySize=7g
spark.driver.extraClassPath /opt/omni-operator/lib/gluten-omni-bundle-spark3.2_2.12-openEuler_22.03_aarch_64-1.3.0-2.2.0.jar:/opt/omni-operator/lib/boostkit-omniop-bindings-2.2.0-aarch64.jar
spark.driver.extraLibraryPath /opt/omni-operator/lib
spark.driverEnv.LD_LIBRARY_PATH /opt/omni-operator/lib
spark.driverEnv.OMNI_HOME /opt/omni-operator
spark.driverEnv.LD_PRELOAD /opt/omni-operator/lib/libjemalloc.so.2
spark.driverEnv.MALLOC_CONF narenas:2,tcache:false
spark.executor.extraClassPath /opt/omni-operator/lib/gluten-omni-bundle-spark3.2_2.12-openEuler_22.03_aarch_64-1.3.0-2.2.0.jar:/opt/omni-operator/lib/boostkit-omniop-bindings-2.2.0-aarch64.jar
spark.executor.extraLibraryPath /opt/omni-operator/lib
spark.executorEnv.LD_LIBRARY_PATH /opt/omni-operator/lib:/usr/local/lib
spark.executorEnv.OMNI_HOME /opt/omni-operator
spark.executorEnv.LD_PRELOAD /opt/omni-operator/lib/libjemalloc.so.2
spark.executorEnv.MALLOC_CONF narenas:2,tcache:false
spark.plugins org.apache.gluten.GlutenPlugin
spark.shuffle.manager org.apache.spark.shuffle.sort.ColumnarShuffleManager
spark.gluten.sql.columnar.backend.lib omni
spark.gluten.sql.columnar.libpath /opt/omni-operator/lib/libspark_columnar_plugin.so
spark.gluten.sql.native.union true
spark.gluten.sql.native.writer.enabled true
spark.sql.optimizer.runtime.bloomfilter.enabled true
spark.sql.ansi.enabled false
spark.gluten.sql.columnar.backend.omni.combineJoinedAggregates true
spark.gluten.sql.columnar.backend.omni.joinReorderEnhance true
spark.gluten.sql.columnar.backend.omni.dedupLeftSemiJoin true
spark.gluten.sql.columnar.backend.omni.pushOrderedLimitThroughAggEnable true
spark.gluten.sql.columnar.backend.omni.adaptivePartialAggregation.enabled true
spark.gluten.sql.columnar.backend.omni.filterMerge true
spark.gluten.sql.columnar.backend.omni.vec.predicate.enabled true
spark.gluten.sql.columnar.backend.omni.aggregationSpillEnabled true
spark.gluten.sql.columnar.physicalJoinOptimizeEnable true
spark.gluten.sql.columnar.physicalJoinOptimizationLevel 19
spark.gluten.sql.columnar.backend.omni.rewriteSelfJoinInInPredicate true
spark.gluten.sql.columnar.backend.omni.preferVectorizationExpression true
spark.gluten.sql.columnar.filesSplitSingleFile.enabled true
spark.gluten.sql.adaptive.costEvaluator.enabled false
spark.gluten.sql.columnar.backend.omni.rowShuffle.enabled true
spark.gluten.sql.columnar.backend.omni.preferShuffledHashJoin true
spark.gluten.sql.columnar.forceShuffledHashJoin true
spark.gluten.sql.columnar.shuffledHashJoin true
spark.gluten.sql.columnar.sortMergeJoin true
spark.gluten.sql.columnar.backend.omni.orderBySpillEnabled true
spark.gluten.sql.columnar.backend.omni.spill.memFraction 50
spark.gluten.sql.columnar.maxBatchSize 2048
spark.sql.orc.columnarReaderBatchSize 2048
spark.gluten.sql.columnar.maxBatchRowCount 2048
spark.gluten.sql.columnar.backend.omni.shuffleSpillBatchRowNum 2048
```

### 8.2 启动 spark-sql

```bash
export OMNI_HOME=/opt/omni-operator
export LD_LIBRARY_PATH="$OMNI_HOME/lib:${LD_LIBRARY_PATH}"
export LD_PRELOAD="$OMNI_HOME/lib/libjemalloc.so.2"
export SPARK_HOME=/usr/local/spark
"$SPARK_HOME/bin/spark-sql" \
  --master yarn \
  --deploy-mode client \
  --properties-file "$SPARK_HOME/conf/spark-defaults-omni.conf"
```

### 8.3 启动后验证与调试

#### 检查 Spark 配置

进入 `spark-sql` 后执行：

```sql
SET spark.plugins;
SET spark.shuffle.manager;
SET spark.gluten.sql.columnar.backend.lib;
SET spark.gluten.sql.columnar.libpath;
```

输出应包含 `org.apache.gluten.GlutenPlugin`、`org.apache.spark.shuffle.sort.ColumnarShuffleManager`、`omni`，以及 `/opt/omni-operator/lib/libspark_columnar_plugin.so`。

#### 查看执行计划

使用实际业务表执行：

```sql
SET spark.sql.adaptive.enabled=false;
EXPLAIN FORMATTED
SELECT COUNT(*)
FROM <database>.<table>;
SET spark.sql.adaptive.enabled=true;
```

在执行计划中检查是否出现 `Omni` 开头或 `Transformer` 结尾的算子。出现这些算子通常表示 Gluten 已接管对应的列式执行；不支持的算子可能局部回退到 Spark 开源算子。

#### 运行查询

```sql
SELECT COUNT(*)
FROM <database>.<table>;
```

若任务失败，按以下顺序检查：

1. `spark-sql --version` 是否为预期 Spark/Scala 版本；
2. Driver 和 Executor 的 `OMNI_HOME` 是否一致；
3. `spark.gluten.sql.columnar.libpath` 指向的 `.so` 是否存在；
4. `ldd libspark_columnar_plugin.so` 是否存在 `not found`；
5. YARN container 日志中是否出现 `UnsatisfiedLinkError`、`libjemalloc.so.2` 或 bundle jar 找不到；
6. 若仅部分算子没有 `Omni`/`Transformer`，先确认是否属于支持范围。

## 9. 正确性验证

启动验证通过后，使用全量功能用例分别运行 Spark row path 和 Omni columnar path，对比：

- 结果是否一致。

## 10. 本次 Spark 3.2.0 的适配流程

### 10.1 明确适配目标

本次采用替换模式，将 `spark-3.2` profile 的主版本由 Spark 3.2.2 替换为 Spark 3.2.0，复用 `shims/spark32`，不新增并行 profile 或另一套 shim。

Windows 本地代码目录为：

```text
Gluten: C:/git/Gluten-poc330
OmniOperator: C:/git/OmniOperator-poc330
分支: 2026_930_poc
```

### 10.2 修改版本配置和兼容代码

1. 将 `Gluten/pom.xml` 的 `spark-3.2` profile 中 `spark.version` 从 `3.2.2` 改为 `3.2.0`。
2. 将 spark32 的 `SparkShimProvider.DESCRIPTOR` 改为 `SparkShimDescriptor(3, 2, 0)`。
3. 根据编译错误适配 `shims/spark32` 的 Parquet、CTE、V1 write 等 API，并在 `shims/common` 中补齐公共接口。
4. 修改 `gluten-core`、`gluten-substrait` 和 `backends-omni` 中直接依赖高版本 Spark API 的调用，将版本差异通过 shim 或兼容处理隔离。
5. 处理 Spark 3.2 的依赖和打包问题，包括移除不可用的 `delta-spark` package 依赖，以及为有意覆盖的 adaptive shim 类添加精确的 duplicate-class 忽略项。

新增 duplicate-class 忽略项只能解除构建阻断，仍需检查 shade 后的类及运行时 ABI，不能据此认定运行兼容。

### 10.3 提交代码并通过脚本编译

每轮修改后，在 Windows 检查并提交本次相关文件，push 到服务器可获取的同一远端分支。随后在 Linux 执行第 6 章的统一构建脚本，通过 `--fetch_and_reset` 获取提交；不额外上传源码、patch 或临时脚本。

按本文当前部署方式，编译命令为：

```bash
cd /opt/omni-operator-build
bash build_omnioperator.sh --docker compile_omnioperator \
  --omni_home /opt/omni-operator-build/omni-operator \
  --spark spark3.2 \
  --fetch_and_reset
```

编译失败时，按第 5 章判断修改位置，在 Windows 修改、提交、推送后重新执行脚本，直至 native 和完整 Maven reactor 构建成功。随后按第 7 章检查 bundle、bindings jar、SO 和重复类。

### 10.4 本次构建记录和后续验证

| 项目 | 已确认结果 |
| --- | --- |
| Gluten commit | `3ba197284`，适配提交已推送 |
| OmniOperator commit | `827314e8`，本次未修改 OmniOperator 源码 |
| 构建完成时间 | 2026-09-22 21:03（北京时间） |
| OmniOperator release 和 Gluten native 编译 | 成功 |
| Spark 3.2 shim、`gluten-core`、`gluten-substrait`、`backends-omni`、package | 完整 Maven reactor `BUILD SUCCESS` |
| bundle | `gluten-omni-bundle-spark3.2_2.12-openEuler_22.03_aarch_64-1.3.0-2.2.0.jar`，已核验生成 |
| native 和 Java bindings | `libspark_columnar_plugin.so`、`boostkit-omniop-bindings-2.2.0-aarch64.jar`，已核验生成 |

上述结果证明本次编译和打包通过。目标 Spark 3.2.0 的运行验证、全量功能用例正确性对比和其他仍受支持版本线的回归，尚未在本次记录中确认通过。

后续按第 8 章配置 Spark 并启动查询，按第 9 章对比 Spark row path 与 Omni columnar path 的全量功能用例结果。由于本次修改了公共接口和后端代码，还需使用统一构建脚本的 `--spark spark3.3`、`--spark spark3.4`、`--spark spark3.5` 对实际仍受支持的版本线做回归编译，并根据受影响路径补充运行验证。全部通过后，才记录为适配完成。
