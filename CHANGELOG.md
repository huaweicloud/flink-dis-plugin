# 1.0.1

- Features
  * 支持在客户端给任务分配分区
  
# 1.0.3
- Bugfixs
  * App不存在时自动创建
  
# 1.0.4
- Features
  * 支持通道缩容
  
# 1.0.5
- Bugfixs
  * 修复在Checkpoint为空时且`auto.offset.reset`配置为`earliest`时无法提交Checkpoint的问题

# 1.0.6
- Bugfixs
  * 修复在部分 Rebalance 场景下分区 Offset 可能被重置为 `earliest` 对应的 Offset 的问题