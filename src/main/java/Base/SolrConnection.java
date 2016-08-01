package Base;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.io.IOException;

public class SolrConnection {

    public static void main(String[] args) throws SolrServerException, IOException {
        String zkHost = "cmagent1:2181";
        QueryResponse response;
        SolrDocumentList list;
        CloudSolrClient.Builder server = new CloudSolrClient.Builder();
        server.withZkHost(zkHost);
        server.build().setParser(new XMLResponseParser());
        SolrQuery parameters = new SolrQuery();
        parameters.set("q", "*:*");
        parameters.set("qt", "/select");
        parameters.set("collection", "batch");
        response = server.build().query(parameters);
        list = response.getResults();
        System.out.println(list.size());
    }
}