package Base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class QUERYHBASEFSOLR {
    public static void main(String[] args) throws SolrServerException, IOException,JSONException {
        final Configuration conf;
        conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "cmagent1");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.setProperty("hadoop.home.dir", "C:\\src\\hadoop-2.6.4");

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table =  conn.getTable(TableName.valueOf("batch_content"));
        Get get;
        List<Get> list = new ArrayList<Get>();

        String url = "http://cmagent3:8983/solr/batch_shard11_replica3";
        SolrServer server = new HttpSolrServer(url);
       // SolrQuery query = new SolrQuery("id:000area-KgW7mg");
        SolrQuery query = new SolrQuery("HContent:我们");
        query.setStart(0); //数据起始行，分页用
        query.setRows(10); //返回记录数，分页用
        QueryResponse response = server.query(query);
        SolrDocumentList docs = response.getResults();
        System.out.println("文档个数：" + docs.getNumFound()); //数据总条数也可轻易获取
        System.out.println("查询时间：" + response.getQTime());
        for (SolrDocument doc : docs) {
            get = new Get(Bytes.toBytes((String) doc.getFieldValue("id")));
            list.add(get);
        }

        System.out.println(list);
        AtomicReference<List<String>> tt = new AtomicReference<List<String>>(new ArrayList<String>());
        for (Get aList : list) {
            if (aList != null) {
                String st = aList.toString();
                JSONObject jsonlist = new JSONObject(st);
                //System.out.println(jsonlist.get("row"));
                String row = (String) jsonlist.get("row");
                tt.get().add(row);
            }
        }

        Result[] res = table.get(list);

        byte[] bt1;
        byte[] bt2;
        byte[] bt3;
        byte[] bt4;
        String str1;
        String str2;
        String str3;
        String str4;
        for (Result rs : res) {
            bt1 = rs.getValue("info".getBytes(),"content".getBytes());
            bt2 = rs.getValue("info".getBytes(), "time".getBytes());
            bt3 = rs.getValue("info".getBytes(), "comment".getBytes());
            bt4 = rs.getValue("info".getBytes(), "frequency".getBytes());
            if (bt1 != null && bt1.length > 0) {
                str1 = new String(bt1);
            } else {
                str1 = "无数据";
            } //对空值进行new String的话会抛出异常
            if (bt2 != null && bt2.length > 0) {
                str2 = new String(bt2);
            } else {
                str2 = "无数据";
            }
            if (bt3 != null && bt3.length > 0) {
                str3 = new String(bt3);
            } else {
                str3 = "无数据";
            }
            if (bt4 != null && bt4.length > 0) {
                str4 = new String(bt4);
            } else {
                str4 = "无数据";
            }
            //System.out.print(new String(rs.getRow()) + " ");
            System.out.print(str1 + "|");
            System.out.print(str2 + "|");
            System.out.print(str3 + "|");
            System.out.println(str4 + "|");
        }
        table.close();

    }
}