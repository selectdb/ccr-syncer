# 使用须知

**ccr syncer 支持的最小 doris 版本是 2.1，2.0 版本将不再支持。**

## 建议打开的配置

- `restore_reset_index_id`：如果要同步的表中带有 inverted index，那么必须在目标集群上配置为 `false`。
- `ignore_backup_tmp_partitions`：如果上游有创建 tmp partition，那么 doris 会禁止做 backup，因此 ccr-syncer 同步会中断；通过在 FE 设置 `ignore_backup_tmp_partitions=true` 可以避免这个问题。

## 注意事项

- CCR 同步期间 backup/restore job 和 binlogs 都在 FE 内存中，因此建议在 FE 给每个 ccr job 都留出 4GB 及以上的堆内存（源和目标集群都需要），同时注意修改下列配置减少无关 job 对内存的消耗：
    - 修改 FE 配置 `max_backup_restore_job_num_per_db`:
        记录在内存中的每个 DB 的 backup/restore job 数量。默认值是 10，设置为 2 就可以了。
    - 修改源集群 db/table property，设置 binlog 保留限制
        - `binlog.max_bytes`: binlog 最大占用内存，建议至少保留 4GB（默认无限制）
        - `binlog.ttl_seconds`: binlog 保留时间，从 2.0.5 之前的老版本默认无限制；之后的版本默认值为一天（86400）
        比如要修改 binlog ttl seconds 为保留一个小时: `ALTER TABLE  table SET ("binlog.ttl_seconds"="3600")`
- CCR 正确性依也赖于目标集群的事务状态，因此要保证在同步过程中事务不会过快被回收，需要调大下列配置
    - `label_num_threshold`：用于控制 TXN Label 数量
    - `stream_load_default_timeout_second`：用于控制 TXN 超时时间
    - `label_keep_max_second`: 用于控制 TXN 结束后保留时间
    - `streaming_label_keep_max_second`：同上
- 如果是 db 同步且源集群的 tablet 数量较多，那么产生的 ccr job 可能非常大，需要修改几个 FE 的配置：
    - `max_backup_tablets_per_job`:
        一次 backup 任务涉及的 tablet 上限，需要根据 tablet 数量调整（默认值为 30w，过多的 tablet 数量会有 FE OOM 风险，优先考虑能否降低 tablet 数量）
    - `thrift_max_message_size`:
        FE thrift server 允许的单次 RPC packet 上限，默认值为 100MB，如果 tablet 数量太多导致 snapshot info 大小超过 100MB，则需要调整该限制，最大 2GB
        - Snapshot info 大小可以从 ccr syncer 日志中找到，关键字：`snapshot response meta size: %d, job info size: %d`，snapshot info 大小大约是 meta size + job info size。
    - `fe_thrift_max_pkg_bytes`:
        同上，一个额外的参数，2.0 中需要调整，默认值为 20MB
    - `restore_download_task_num_per_be`:
        发送给每个 BE download task 数量上限，默认值是 3，对 restore job 来说太小了，需要调整为 0（也就是关闭这个限制）； 2.1.8 和 3.0.4 起不再需要这个配置。
    - `backup_upload_task_num_per_be`:
        发送给每个 BE upload task 数量上限，默认值是 3，对 backup job 来说太小了，需要调整为 0 （也就是关闭这个限制）；2.1.8 和 3.0.4 起不再需要这个配置。
    - 除了上述 FE 的配置外，如果 ccr job 的 db type 是 mysql，还需要调整 mysql 的一些配置：
        - mysql 服务端会限制单次 select/insert 返回/插入数据包的大小。增加下列配置以放松该限制，比如调整到上限 1GB
        ```
        [mysqld]
        max_allowed_packet = 1024MB
        ```
        - mysql client 也会有该限制，在 ccr syncer 2.1.6/2.0.15 及之前的版本，上限为 128MB；之后的版本可以通过参数 `--mysql_max_allowed_packet` 调整（单位 bytes），默认值为 1024MB
        > 注：在 2.1.8 和 3.0.4 以后，ccr syncer 不再将 snapshot info 保存在 db 中，因此默认的数据包大小已经足够了。
- 同上，BE 端也需要修改几个配置
    - `thrift_max_message_size`: BE thrift server 允许的单次 RPC packet 上限，默认值为 100MB，如果 tablet 数量太多导致 agent task 大小超过 100MB，则需要调整该限制，最大 2GB
    - `be_thrift_max_pkg_bytes`：同上，只有 2.0 中需要调整的参数，默认值为 20MB
- 即使修改了上述配置，当 tablet 继续上升时，产生的 snapshot 大小可能会超过 2GB，也就是 doris FE edit log 和 RPC message size 的阈值，导致同步失败。从 2.1.8 和 3.0.4 开始，doris 可以通过压缩 snapshot 来进一步提高备份恢复支持的 tablet 数量。可以通过下面几个参数开启压缩：
    - `restore_job_compressed_serialization`: 开启对 restore job 的压缩（影响元数据兼容性，默认关闭）
    - `backup_job_compressed_serialization`: 开启对 backup job 的压缩（影响元数据兼容性，默认关闭）
    - `enable_restore_snapshot_rpc_compression`: 开启对 snapshot info 的压缩，主要影响 RPC（默认开启）
    > 注：由于识别 backup/restore job 是否压缩需要额外的代码，而 2.1.8 和 3.0.4 之前的代码中不包含相关代码，因此一旦有 backup/restore job 生成，那么就无法回退到更早的 doris 版本。有两种情况例外：已经 cancel 或者 finished 的 backup/restore job 不会被压缩，因此在回退前等待 backup/restore job 完成或者主动取消 job 后，就能安全回退。
- Ccr 内部会使用 db/table 名作为一些内部 job 的 label，因此如果 ccr job 中碰到了 label 超过限制了，可以调整 FE 参数 `label_regex_length` 来放松该限制（默认值为 128）
- 由于 backup 暂时不支持备份带有 cooldown tablet 的表，如果碰到了会导致同步终端，因此需要在创建 ccr job 前检查是否有 table 设置了 `storage_policy` 属性。

## 性能相关参数

- 如果用户的数据量非常大，备份、恢复执行完需要的时间可能会超过一天（默认值），那么需要按需调整下列参数
    - `backup_job_default_timeout_ms` 备份/恢复任务超时时间，源、目标集群的 FE 都需要配置
    - 上游修改 binlog 保留时间: `ALTER DATABASE $db SET PROPERTIES ("binlog.ttl_seconds" = "xxxx")`
- 下游 BE 下载速度慢
    - `max_download_speed_kbps` 下游单个 BE 中单个下载线程的下载限速，默认值为 50MB/s
    - `download_worker_count` 下游执行下载任务的线程数，默认值为 1；需要结合客户机型调整，在不影响客户正常读写时跳到最大；如果调整了这个参数，就可以不用调整 `max_download_speed_kbps`。
        - 比如客户机器网卡最大提供 1GB 的带宽，现在最大允许下载线程利用 200MB 的带宽，那么在不改变 `max_download_speed_kbps` 的情况下，`download_worker_count` 应该配置成 4。

## 不建议使用版本

Doris 版本
- 2.1.5/2.0.14：如果从之前的版本升级到这两个版本，且用户有 drop partition 操作，那么会在升级、重启时碰到 NPE，原因是这个版本引入了一个新字段，旧版本没有所以默认值为 null。这个问题在 2.1.6/2.0.15 修复。

