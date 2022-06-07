file connector是flink中一类基础的连接器，截至flink1.15，其共提供了以下五种文件连接器：
- [Avro](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/formats/avro/)
- [Azure table](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/formats/azure_table_storage/)
- [Hadoop](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/formats/hadoop/)
- [parquet](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/formats/parquet/)
- [Text files](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/connectors/datastream/formats/text_files/)
此模块给出最常见的Text files（本地文件）和Hadoop文件系统的使用方法。
---
##### text files
要使此连接器，需要引入依赖：

``` 
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-files</artifactId>
	<version>${flink.version}</version>
</dependency>
```

此连接器提供了连接本地文件系统的能力，可以实现将文件、目录下所有文件作为data source。在读入文件时可以指定定期扫描，为元素指定timestamp等功能。

##### Hadoop
flink提供了连接器去读取hdfs上的文件内容，要将hdfs上的文件作为data source，首先导入依赖：

``` 
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-hadoop-compatibility_${scala.version}</artifactId>
	<version>${flink.version}</version>
</dependency>

<dependency>
	<groupId>org.apache.hadoop</groupId>
	<artifactId>hadoop-client</artifactId>
	<version>3.2.1</version>
	<scope>provided</scope>
</dependency>
```
然后使用`HadoopInputs.readHadoopFile`就可以读取hdfs文件了