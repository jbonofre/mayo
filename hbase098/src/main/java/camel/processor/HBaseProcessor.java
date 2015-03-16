package camel.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseProcessor implements Processor {

    public void process(Exchange exchange) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        // TODO replace static string with in message
        HTable table = new HTable(configuration, "registry-role");
        Get get = new Get(Bytes.toBytes("1"));
        Result result = table.get(get);
        exchange.getOut().setBody(result.toString());
    }

}
