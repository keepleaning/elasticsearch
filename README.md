# ElasticSearch
  此Demo基于Maven构建的Java项目，依赖Jar包配置文件在pom.xml里。
  基于ElasticSearch2.2版本，使用 ElasticSearch JavaAPI编写的此Demo。
  一些基本的方法列举如下：
  1、创建索引：createIndex();
  2、删除索引：deleteIndex();
  3、清空所有索引：deleteAllResponse();
  4、定义索引的映射类型：createIndexTypeMapping();
  5、插入数据：insertData();
  6、批量插入数据：batchInsertData();
  7、删除数据：deleteById();
  8、批量删除数据：batchDeleteData();
  9、更改数据：updateData();
  10、执行脚本：excuteByScript(); //为了安全考虑，不建议开启此功能。script.inline: off
  11、批量更改数据：batchUpdateData();
  12、检索数据：queryData();
  
