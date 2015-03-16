package org.mayo.camel.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class HBaseProcessor implements Processor {

    public void process(Exchange exchange) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        // check operation
        if (exchange.getIn().getHeader("operationName", String.class).equals("GET")) {
            // GET operation
            // get the resource header
            String resource = exchange.getIn().getHeader("qualifier", String.class);
            resource = "registry-" + resource;
            HTable table = new HTable(configuration, resource);
            String id = exchange.getIn().getHeader("id", String.class);
            Get get = new Get(Bytes.toBytes(id));
            Result result = table.get(get);
            List<KeyValue> kvs = result.getColumn(Bytes.toBytes(exchange.getIn().getHeader("CamelHBaseFamily", String.class)), Bytes.toBytes(exchange.getIn().getHeader("CamelHBaseQualifier", String.class)));
            exchange.getIn().setBody(kvs);
        }
    }

}
