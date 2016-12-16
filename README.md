# ElasticSearch
  此Demo基于Maven构建的Java项目，依赖Jar包配置文件在pom.xml里。 <br/>
  基于ElasticSearch2.2版本，使用 ElasticSearch JavaAPI编写的此Demo。 <br/>
  一些基本的方法列举如下：  <br/>
  1、创建索引：createIndex();  <br/>
  2、删除索引：deleteIndex();  <br/>
  3、清空所有索引：deleteAllResponse();  <br />
  4、定义索引的映射类型：createIndexTypeMapping();  <br/>
  5、插入数据：insertData();  <br/>
  6、批量插入数据：batchInsertData();  <br/>
  7、删除数据：deleteById();   <br/>
  8、批量删除数据：batchDeleteData();   <br/>
  9、更改数据：updateData();  <br/>
  10、执行脚本：excuteByScript(); //为了安全考虑，不建议开启此功能。script.inline: off  <br/>
  11、批量更改数据：batchUpdateData();  <br/>
  12、检索数据：queryData();
  
