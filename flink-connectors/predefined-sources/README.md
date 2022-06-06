##### 此模块描述flink预定义的数据源

---
本模块的代码包含以下内容：
1. CollectionSource
   - 从集合中构建DataStream，因为写起来非常方便，适合在开发过程中对代码进行测试。
2. FileSource
   - 从文件中读取内容当作数据源，实际上flink还提供了更文丰富的文件读取方法，其单独为文件读取提供了一个connector，这部分内容放在`flink-connectors/file-connector`中。
3. SocketSource
   - 从端口中读取DataSource
