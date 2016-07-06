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
    // ����client.transport.sniffΪtrue��ʹ�ͻ���ȥ��̽������Ⱥ��״̬���Ѽ�Ⱥ������������ip��ַ�ӵ��ͻ����У�
    static Settings settings = Settings.settingsBuilder().put(m).put("cluster.name", "fuhe-24")
            .put("client.transport.sniff", true).build();

    // ����˽�ж���
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
     * ˵����ȡ��ʵ��
     * 
     * @author pizirui
     */
    public static synchronized TransportClient getTransportClient() {
        return client;
    }

    /**
     * ˵������������ �൱�� ���ݿ��д������ݿ�
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����--------------һ��es��Ⱥ�п����ж�������⡣ ���Ʊ���ΪСд
     * @param alias
     *            ���� �൱�����ݿ��е�as
     * @author pizirui
     */
    public static void createIndex(String IndexName, String alias) {
        client.admin().indices().create(new CreateIndexRequest(IndexName).alias(new Alias(alias))).actionGet();
    }

    public static void createIndex(String IndexName) {
        client.admin().indices().create(new CreateIndexRequest(IndexName)).actionGet();
    }

    /**
     * ˵����ɾ������ �൱�� ���ݿ���ɾ�����ݿ�
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
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
     * �����������
     * 
     * @param client
     * @throws ExecutionException
     * @throws InterruptedException
     * @author zhangsh
     */
    public static ClusterStateResponse deleteAllResponse() throws InterruptedException, ExecutionException {

        ClusterStateResponse response = client.admin().cluster().prepareState().execute().actionGet();
        // ��ȡ��������
        String[] indexs = response.getState().getMetaData().getConcreteAllIndices();
        for (String index : indexs) {
            System.out.println(index + " delete");//
            // �������������

            DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).execute()
                    .actionGet();
            System.out.println(deleteIndexResponse.getHeaders());

        }
        return response;
    }

    /**
     * ˵��������������ӳ������ �൱�� ���ݿ��д���һ�ű� ע�⣺����û��ɾ��type����Ч�취���������ʱ��һ��indexֻ�����һ��type
     * ������������˲鵽ɾ��type�İ취�����֪��
     * ��𣺴�2.0��ʼ��֧��ɾ��type
     *     �ٷ����ͣ�It is no longer possible to delete the mapping for a type. 
     *           Instead you should delete the index and recreate it with the new mappings.
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @author zhangsh
     */
    public static void createIndexTypeMapping(String IndexName, String TypeName) {
        // _all ElasticSarch�Զ�ʹ��_all���е��ĵ����򶼻ᱻ�ӵ�_all�н�������
        // type �ֶ����� string,date,long, integer, short, byte, double,
        // float,boolean,binary
        // store �Ƿ�洢 yes no
        // index �Ƿ����� no ������ not_analyzed ���ִ����� analyzed �ִ�����
        // analyzer ʹ�÷ִʵĲ�� ����ʹ��ik Ӣ��ʹ��Ĭ��
        // include_in_all ȫ�ֶμ���ʱ�Ƿ�������ֶ� true false Ĭ����true
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
         * catch (IOException e) { // �쳣���� }
         */
        try {
            XContentBuilder mapBuilder = XContentFactory.jsonBuilder();
            mapBuilder.startObject().startObject(TypeName).startObject("_all").field("enabled", "true").endObject();
            PutMappingRequest putMappingRequest = Requests.putMappingRequest(IndexName).type(TypeName)
                    .source(mapBuilder);
            client.admin().indices().putMapping(putMappingRequest).actionGet();
        } catch (IOException e) {
            // �쳣����
        }
    }

    /**
     * ˵������������
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @param jsonObject
     *            ���� json���� ����������id
     *         setRefresh
     *            ������Ƿ�ˢ�£�Ĭ��false
     *            setVersion
     *            ������
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
            // �������
            return true;
        }
        return false;
    }

    /**
     * ˵����������������
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @param jsonObjectList
     *            ���ݼ��� json���� ����������id
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
            // �������
            return false;
        }
        return true;
    }

    /**
     * ˵����ɾ������
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @param id
     * @author pizirui
     */
    public static boolean deleteById(String IndexName, String TypeName, String id) {

        DeleteResponse response = client.prepareDelete(IndexName, TypeName, id).execute().actionGet();

        if (response.isFound()) {
            // �������
            return true;
        }
        return false;
    }

    /**
     * ˵����ɾ������
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @param idList
     *            id����
     * @author pizirui
     */
    public static boolean batchDeleteData(String IndexName, String TypeName, List<String> idList) {

        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String id : idList) {
            bulkRequest.add(client.prepareDelete(IndexName, TypeName, id));
        }

        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
            // �������
            return false;
        }
        return true;
    }

    /**
     * ˵������������
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @param jsonObject
     *            ���� json���� ����������id
     * @author pizirui
     */
    public static boolean updateData(String IndexName, String TypeName, JSONObject jsonObject) {

        // �첽������ʱִ�� response.actionGet();
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

        // �첽������ʱִ�� response.actionGet();
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
     * ˵����ִ�нű�
     * @param IndexName
     * @param TypeName
     * @param jsonObject
     * @return boolean
     * ������У� "script" : "ctx._source.name_of_new_field = \"value_of_new_field\""
     * ɾ���У�"script" : "ctx._source.remove(\"name_of_field\")"
     * �����ˣ� zhangsh  ���ڣ�2016��5��11��
     * �޸��ˣ�  ���ڣ�
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
     * ˵����������������
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @param jsonObjectList
     *            ���ݼ��� json���� ����������id
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
            // �������
            return false;
        }
        return true;
    }

    /**
     * ˵���������ѯ���� from �ӵڼ������ݿ�ʼ�� size ��ѯ���������� sort ��ĳ���ֶν�������
     * 
     * @param IndexName
     *            ���� �൱�����ݿ��е����ݿ�����
     * @param TypeName
     *            ���� �൱�����ݿ��еı������
     * @param matchQueryBuilder
     *            ��ѯ��
     * @author pizirui
     */
    public static SearchResponse queryData(String IndexName, String TypeName, MatchQueryBuilder matchQueryBuilder) {
        SortBuilder sortBuilder = SortBuilders.fieldSort("id").order(SortOrder.ASC);
        return client.prepareSearch(IndexName).setTypes(TypeName).setQuery(matchQueryBuilder).addSort(sortBuilder)
                .setFrom(0).setSize(20).execute().actionGet();
    }

    public static void main(String[] args) {
        // //ɾ������
        deleteIndex("edian");
        // ��������
        createIndex("edian");
        // ����type
        createIndexTypeMapping("edian", "eposition");
        // //��������
        // EUser user = new EUser("15764229700", "��˧", "123456", "D:/cd", 1,
        // "1");
        // JSONObject jsonObject = JSONObject.fromObject(user);
        // jsonObject.put("parentId", "no");
        // jsonObject.put("id", user.getUserTel());
        // insertData("edian", "euser", jsonObject);
        // //������������
        // List<JSONObject> jsonObjectList = new ArrayList<JSONObject>();
        // for (int i = 1; i < 100; i++) {
        // User a = new User("�����й���" + i, "asd");
        // JSONObject b = JSONObject.fromObject(a);
        // b.put("id", i);
        // b.put("parentId", "no");
        // jsonObjectList.add(b);
        // }
        // System.out.println(batchInsertData("fuhe24", "user",
        // jsonObjectList));
        // //ɾ������
        // deleteData("fuhe24", "user", "1");
        // //����ɾ������
        // List<String> idList = new ArrayList<String>();
        // for (int i = 0; i < 31; i++) {
        // idList.add(String.valueOf(i));
        // }
        // batchDeleteData("fuhe24", "user", idList);
        // ��������
        // User user = new User();
        // user.setDate(new Date());
        // JSONObject jsonObject = JSONObject.fromObject(user);
        // jsonObject.put("id", "3");
        // System.out.println(jsonObject.toString());
        // System.out.println(updateData("fuhe24", "user", jsonObject));
        // �ֲ�����
        // User user = new User();
        //
        // user.setPassword("111");
        // user.setSex(1);
        // JSONObject jsonObject = JSONObject.fromObject(user);
        // jsonObject.put("id", "1");
        // System.out.println(updateDataSelected("fuhe24", "user", jsonObject));
        // ִ�нű�
        // System.out.println(excuteByScript("fuhe24", "user", "3",
        // "ctx._source.remove(\"sex\")"));
        // ������������
        // List<JSONObject> jsonObjectList = new ArrayList<JSONObject>();
        // for(int i=32;i<51;i++){
        // User a = new User("�����й���"+i, "�����й���"+i);
        // JSONObject b = JSONObject.fromObject(a);
        // b.put("id", i);
        // jsonObjectList.add(b);
        // }
        // batchUpdateData("fuhe24","user",jsonObjectList);
        // ��ѯ
        // JSONObject b = null;
        //
        // SearchResponse sp = queryData("fuhe24", "user",
        // QueryBuilders.matchPhrasePrefixQuery("id", "3"));
        // for (SearchHit hits : sp.getHits()) {
        // String sourceAsString = hits.sourceAsString();// ���ַ�����ʽ��ӡ
        // System.out.println(sourceAsString);
        // User user1 = new User();
        // user1 = (User) b.toBean(JSONObject.fromObject(sourceAsString),
        // User.class);
        // System.out.println(user1.getPassword());
        // }

        // ������ѯ
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
        // User a = new User("�����й���"+i, "asd");
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
