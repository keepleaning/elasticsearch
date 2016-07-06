package elasticsearch;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import net.sf.json.JSONObject;

public class ElasticSearch {

    static Map<String, String> m = new HashMap<String, String>();
    // 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中，
    static Settings settings = Settings.settingsBuilder().put(m).put("cluster.name", "fuhe-24")
            .put("client.transport.sniff", true).build();

    // 创建私有对象
    private static TransportClient client;

    static {
        try {
            client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("121.42.12.251"), 9300));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 说明：取得实例
     * 
     * @author pizirui
     */
    public static synchronized TransportClient getTransportClient() {
        return client;
    }

    /**
     * 说明：创建索引 相当于 数据库中创建数据库
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称--------------一个es集群中可以有多个索引库。 名称必须为小写
     * @param alias
     *            别名 相当于数据库中的as
     * @author pizirui
     */
    public static void createIndex(String IndexName, String alias) {
        client.admin().indices().create(new CreateIndexRequest(IndexName).alias(new Alias(alias))).actionGet();
    }

    public static void createIndex(String IndexName) {
        client.admin().indices().create(new CreateIndexRequest(IndexName)).actionGet();
    }

    /**
     * 说明：删除索引 相当于 数据库中删除数据库
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @author pizirui
     */
    public static void deleteIndex(String IndexName) {
        IndicesExistsResponse indicesExistsResponse = client.admin().indices()
                .exists(new IndicesExistsRequest(new String[] { IndexName })).actionGet();
        if (indicesExistsResponse.isExists()) {
            client.admin().indices().delete(new DeleteIndexRequest(IndexName)).actionGet();
        }
    }

    /**
     * 清空所有索引
     * 
     * @param client
     * @throws ExecutionException
     * @throws InterruptedException
     * @author zhangsh
     */
    public static ClusterStateResponse deleteAllResponse() throws InterruptedException, ExecutionException {

        ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet();
        // 获取所有索引
        String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
        for (String index : indexs) {
            System.out.println(index + " delete");//
            // 清空所有索引。

            DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).execute()
                    .actionGet();
            System.out.println(deleteIndexResponse.getHeaders());

        }
        return response;
    }

    /**
     * 说明：定义索引的映射类型 相当于 数据库中创建一张表 注意：由于没有删除type的有效办法。所以设计时，一个index只能设计一个type
     * 求助：如果有人查到删除type的办法，请告知。
     * 解答：从2.0开始不支持删除type
     *     官方解释：It is no longer possible to delete the mapping for a type. 
     *           Instead you should delete the index and recreate it with the new mappings.
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @author zhangsh
     */
    public static void createIndexTypeMapping(String IndexName, String TypeName) {
        // _all ElasticSarch自动使用_all所有的文档的域都会被加到_all中进行索引
        // type 字段类型 string,date,long, integer, short, byte, double,
        // float,boolean,binary
        // store 是否存储 yes no
        // index 是否索引 no 不索引 not_analyzed 不分词索引 analyzed 分词索引
        // analyzer 使用分词的插件 中文使用ik 英文使用默认
        // include_in_all 全字段检索时是否包含此字段 true false 默认是true
        /*
         * try { XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
         * mapBuilder.startObject().startObject(TypeName).startObject("_all").
         * field("enabled", "true").endObject()
         * .startObject("properties").startObject("username").field("type",
         * "string").field("store", "no") .field("index",
         * "analyzed").field("analyzer",
         * "ik").endObject().startObject("password") .field("type",
         * "string").field("store", "no").field("index",
         * "not_analyzed").endObject().endObject() .endObject().endObject();
         * PutMappingRequest putMappingRequest =
         * Requests.putMappingRequest(IndexName).type(TypeName)
         * .source(mapBuilder);
         * client.admin().indices().putMapping(putMappingRequest).actionGet(); }
         * catch (IOException e) { // 异常处理 }
         */
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject().startObject(TypeName).startObject("_all").field("enabled", "true").endObject();
            PutMappingRequest putMappingRequest = Requests.putMappingRequest(IndexName).type(TypeName)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            // 异常处理
        }
    }

    /**
     * 说明：插入数据
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @param jsonObject
     *            数据 json对象 里面必须包含id
     *         setRefresh
     *            插入后是否刷新，默认false
     *            setVersion
     *            处理并发
     * @author zhangsh
     */
    public static boolean insertData(String IndexName, String TypeName, JSONObject jsonObject) {
        String jsonData = JSONObject.fromObject(jsonObject).toString();
        IndexResponse response = null;
        if ("no".equals(jsonObject.getString("parentId"))) {
            response = client.prepareIndex(IndexName, TypeName, jsonObject.getString("id")).setSource(jsonData)
                    .setRefresh(true).execute().actionGet();

        } else {
            response = client.prepareIndex(IndexName, TypeName, jsonObject.getString("id"))
                    .setParent(jsonObject.getString("parentId")).setSource(jsonData).setRefresh(true).execute()
                    .actionGet();
        }

        if (response.isCreated()) {
            // 处理错误
            return true;
        }
        return false;
    }

    /**
     * 说明：批量插入数据
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @param jsonObjectList
     *            数据集合 json对象 里面必须包含id
     * @author pizirui
     */
    public static boolean batchInsertData(String IndexName, String TypeName, List<JSONObject> jsonObjectList) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (JSONObject jsonObject : jsonObjectList) {
            if ("no".equals(jsonObject.getString("parentId"))) {
                bulkRequest.add(client.prepareIndex(IndexName, TypeName, jsonObject.getString("id"))
                        .setSource(jsonObject.toString()));
            } else {
                bulkRequest.add(client.prepareIndex(IndexName, TypeName, jsonObject.getString("parentId"))
                        .setParent(jsonObject.getString("parentId")).setSource(jsonObject.toString()));
            }

        }

        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
            // 处理错误
            return false;
        }
        return true;
    }

    /**
     * 说明：删除数据
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @param id
     * @author pizirui
     */
    public static boolean deleteById(String IndexName, String TypeName, String id) {

        DeleteResponse response = client.prepareDelete(IndexName, TypeName, id).execute().actionGet();

        if (response.isFound()) {
            // 处理错误
            return true;
        }
        return false;
    }

    /**
     * 说明：删除数据
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @param idList
     *            id集合
     * @author pizirui
     */
    public static boolean batchDeleteData(String IndexName, String TypeName, List<String> idList) {

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String id : idList) {
            bulkRequest.add(client.prepareDelete(IndexName, TypeName, id));
        }

        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
            // 处理错误
            return false;
        }
        return true;
    }

    /**
     * 说明：更改数据
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @param jsonObject
     *            数据 json对象 里面必须包含id
     * @author pizirui
     */
    public static boolean updateData(String IndexName, String TypeName, JSONObject jsonObject) {

        // 异步，可随时执行 response.actionGet();
        // String a = null;
        // @SuppressWarnings("rawtypes")
        // Iterator it = jsonObject.keys();
        // while (it.hasNext()) {
        // String key = (String) it.next();
        // String value = jsonObject.getString(key);
        // if (!value.equals("null") && !value.equals("0") && !value.equals(""))
        // {
        // a += "ctx._source." + key + "=" + value;
        // }
        // }
        // System.out.println(a);
        UpdateRequest request = new UpdateRequest();
        request.index(IndexName).type(TypeName).id(jsonObject.getString("id")).refresh(true).doc(jsonObject.toString());
        ActionFuture<UpdateResponse> response = client.update(request);

        response.actionGet();
        if (response.isDone()) {
            return true;
        }
        return false;

        // UpdateResponse response = client.prepareUpdate(IndexName, TypeName,
        // jsonObject.getString("id"))
        // .setDoc(jsonObject.toString()).setRefresh(true).execute().actionGet();
        //
        // System.out.println(response.getShardInfo());

    }

    @SuppressWarnings("deprecation")
    public static boolean updateDataSelected(String IndexName, String TypeName, JSONObject jsonObject) {

        // 异步，可随时执行 response.actionGet();
        String script = "";
        @SuppressWarnings("rawtypes")
        Iterator it = jsonObject.keys();
        while (it.hasNext()) {
            String key = (String) it.next();
            String value = jsonObject.getString(key);
            if (!key.equals("id") && !value.equals("null") && !value.equals("0") && !value.equals("")) {
                script += "ctx._source." + key + "=" + value + " ; ";
            }
        }
        script = script.substring(0, script.length() - 3);
        UpdateRequest request = new UpdateRequest();
        request.index(IndexName).type(TypeName).id(jsonObject.getString("id")).script(script).refresh(true);
        ActionFuture<UpdateResponse> response = client.update(request);

        response.actionGet();
        if (response.isDone()) {
            return true;
        }
        return false;

    }

    /**
     * 
     * 说明：执行脚本
     * @param IndexName
     * @param TypeName
     * @param jsonObject
     * @return boolean
     * 添加新列： "script" : "ctx._source.name_of_new_field = \"value_of_new_field\""
     * 删除列："script" : "ctx._source.remove(\"name_of_field\")"
     * 创建人： zhangsh  日期：2016年5月11日
     * 修改人：  日期：
     */
    @SuppressWarnings("deprecation")
    public static boolean excuteByScript(String IndexName, String TypeName, String id, String script) {
        UpdateRequest request = new UpdateRequest();
        request.index(IndexName).type(TypeName).id(id).script(script).refresh(true);
        ActionFuture<UpdateResponse> response = client.update(request);

        response.actionGet();
        if (response.isDone()) {
            return true;
        }
        return false;

    }

    /**
     * 说明：批量更改数据
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @param jsonObjectList
     *            数据集合 json对象 里面必须包含id
     * @author pizirui
     */
    public static boolean batchUpdateData(String IndexName, String TypeName, List<JSONObject> jsonObjectList) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (JSONObject jsonObject : jsonObjectList) {
            bulkRequest.add(client.prepareUpdate(IndexName, TypeName, jsonObject.getString("id")).setRefresh(true)
                    .setDoc(jsonObject.toString()));
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            // 处理错误
            return false;
        }
        return true;
    }

    /**
     * 说明：单表查询数据 from 从第几条数据开始， size 查询多少条数据 sort 按某个字段进行排序
     * 
     * @param IndexName
     *            索引 相当于数据库中的数据库名称
     * @param TypeName
     *            类型 相当于数据库中的表的名称
     * @param matchQueryBuilder
     *            查询器
     * @author pizirui
     */
    public static SearchResponse queryData(String IndexName, String TypeName, MatchQueryBuilder matchQueryBuilder) {
        SortBuilder sortBuilder = SortBuilders.fieldSort("id").order(SortOrder.ASC);
        return client.prepareSearch(IndexName).setTypes(TypeName).setQuery(matchQueryBuilder).addSort(sortBuilder)
                .setFrom(0).setSize(20).execute().actionGet();
    }

    public static void main(String[] args) {
        // //删除索引
        deleteIndex("edian");
        // 创建索引
        createIndex("edian");
        // 创建type
        createIndexTypeMapping("edian", "eposition");
        // //插入数据
        // EUser user = new EUser("15764229700", "张帅", "123456", "D:/cd", 1,
        // "1");
        // JSONObject jsonObject = JSONObject.fromObject(user);
        // jsonObject.put("parentId", "no");
        // jsonObject.put("id", user.getUserTel());
        // insertData("edian", "euser", jsonObject);
        // //批量插入数据
        // List<JSONObject> jsonObjectList = new ArrayList<JSONObject>();
        // for (int i = 1; i < 100; i++) {
        // User a = new User("我是中国人" + i, "asd");
        // JSONObject b = JSONObject.fromObject(a);
        // b.put("id", i);
        // b.put("parentId", "no");
        // jsonObjectList.add(b);
        // }
        // System.out.println(batchInsertData("fuhe24", "user",
        // jsonObjectList));
        // //删除数据
        // deleteData("fuhe24", "user", "1");
        // //批量删除数据
        // List<String> idList = new ArrayList<String>();
        // for (int i = 0; i < 31; i++) {
        // idList.add(String.valueOf(i));
        // }
        // batchDeleteData("fuhe24", "user", idList);
        // 更改数据
        // User user = new User();
        // user.setDate(new Date());
        // JSONObject jsonObject = JSONObject.fromObject(user);
        // jsonObject.put("id", "3");
        // System.out.println(jsonObject.toString());
        // System.out.println(updateData("fuhe24", "user", jsonObject));
        // 局部更新
        // User user = new User();
        //
        // user.setPassword("111");
        // user.setSex(1);
        // JSONObject jsonObject = JSONObject.fromObject(user);
        // jsonObject.put("id", "1");
        // System.out.println(updateDataSelected("fuhe24", "user", jsonObject));
        // 执行脚本
        // System.out.println(excuteByScript("fuhe24", "user", "3",
        // "ctx._source.remove(\"sex\")"));
        // 批量更改数据
        // List<JSONObject> jsonObjectList = new ArrayList<JSONObject>();
        // for(int i=32;i<51;i++){
        // User a = new User("我是中国人"+i, "我是中国人"+i);
        // JSONObject b = JSONObject.fromObject(a);
        // b.put("id", i);
        // jsonObjectList.add(b);
        // }
        // batchUpdateData("fuhe24","user",jsonObjectList);
        // 查询
        // JSONObject b = null;
        //
        // SearchResponse sp = queryData("fuhe24", "user",
        // QueryBuilders.matchPhrasePrefixQuery("id", "3"));
        // for (SearchHit hits : sp.getHits()) {
        // String sourceAsString = hits.sourceAsString();// 以字符串方式打印
        // System.out.println(sourceAsString);
        // User user1 = new User();
        // user1 = (User) b.toBean(JSONObject.fromObject(sourceAsString),
        // User.class);
        // System.out.println(user1.getPassword());
        // }

        // 关联查询
        // try {
        //
        // XContentBuilder mapBuilder1 = XContentFactory.jsonBuilder();
        //
        // mapBuilder1.startObject().startObject("userinfo")
        // .startObject("_all").field("enabled", "true").endObject()
        // .startObject("_parent").field("type", "user").endObject()
        // .startObject("properties").startObject("ageInfo")
        // .field("type", "string").field("store", "no")
        // .field("index", "not_analyzed").endObject().endObject()
        // .endObject().endObject();
        //
        // PutMappingRequest putMappingRequest1 = Requests
        // .putMappingRequest("fuhe24").type("userinfo")
        // .source(mapBuilder1);
        // client.admin().indices().putMapping(putMappingRequest1).actionGet();
        // XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
        //
        // mapBuilder.startObject().startObject("user").startObject("_all")
        // .field("enabled", "true").endObject()
        // .startObject("properties").startObject("username")
        // .field("type", "string").field("store", "no")
        // .field("index", "analyzed").field("analyzer", "ik")
        // .endObject().startObject("password")
        // .field("type", "string").field("store", "no")
        // .field("index", "not_analyzed").endObject().endObject()
        // .endObject().endObject();
        //
        // PutMappingRequest putMappingRequest = Requests
        // .putMappingRequest("fuhe24").type("user")
        // .source(mapBuilder);
        // client.admin().indices().putMapping(putMappingRequest).actionGet();
        // } catch (Exception e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }

        // List<JSONObject> jsonObjectList = new ArrayList<JSONObject>();
        // for(int i=1;i<51;i++){
        // User a = new User("我是中国人"+i, "asd");
        // JSONObject b = JSONObject.fromObject(a);
        // b.put("id", i);
        // b.put("parentId", "no");
        // jsonObjectList.add(b);
        // }
        // batchInsertData("fuhe24","user",jsonObjectList);
        //
        // JSONObject jsonObject = new JSONObject();
        // jsonObject.put("id", "0");
        // jsonObject.put("parentId", "4");
        // jsonObject.put("ageInfo", "1234");
        // insertData("fuhe24","userinfo",jsonObject);

    }

}
