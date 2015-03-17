package org.mayo.camel.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.util.ObjectHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.mayo.camel.processor.mapping.HeaderMappingStrategy;
import org.mayo.camel.processor.model.HBaseCell;
import org.mayo.camel.processor.model.HBaseData;
import org.mayo.camel.processor.model.HBaseRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class HBaseProcessor implements Processor {

    private final static Logger LOGGER = LoggerFactory.getLogger(HBaseProcessor.class);

    public void process(Exchange exchange) throws Exception {

        LOGGER.info("HBASE: calling processor");

        LOGGER.info("HBASE: creating configuration");
        Configuration configuration = HBaseConfiguration.create();

        String operation = exchange.getIn().getHeader("CamelHBaseOperation", String.class);
        String tableName = exchange.getIn().getHeader("CamelHBaseTableName", String.class);

        LOGGER.info("HBASE: header mapping strategy");
        HeaderMappingStrategy mappingStrategy = new HeaderMappingStrategy();

        HBaseData data = mappingStrategy.resolveModel(exchange.getIn());

        List<Put> putOperations = new LinkedList<Put>();
        List<Delete> deleteOperations = new LinkedList<Delete>();
        List<HBaseRow> getOperationResult = new LinkedList<HBaseRow>();
        List<HBaseRow> scanOperationResult = new LinkedList<HBaseRow>();

        HTable table = new HTable(configuration, tableName);

        for (HBaseRow hRow : data.getRows()) {
            hRow.apply(new HBaseRow());
            if ("PUT".equalsIgnoreCase(operation)) {
                LOGGER.info("HBASE: PUT");
                putOperations.add(createPut(hRow));
            } else if ("GET".equalsIgnoreCase(operation)) {
                LOGGER.info("HBASE: GET");
                HBaseRow getResultRow = getCells(table, hRow);
                getOperationResult.add(getResultRow);
            } else if ("DELETE".equalsIgnoreCase(operation)) {
                LOGGER.info("HBASE: DELETE");
                deleteOperations.add(createDeleteRow(hRow));
            } else if ("SCAN".equalsIgnoreCase(operation)) {
                LOGGER.warn("HBASE: SCAN NOT SUPPORTED");
                //scanOperationResult = scanCells(table, hRow, endpoint.getFilters());
            }
        }

        //Check if we have something to add.
        if (!putOperations.isEmpty()) {
            table.put(putOperations);
            table.flushCommits();
        } else if (!deleteOperations.isEmpty()) {
            table.delete(deleteOperations);
        } else if (!getOperationResult.isEmpty()) {
            mappingStrategy.applyGetResults(exchange.getOut(), new HBaseData(getOperationResult));
        } else if (!scanOperationResult.isEmpty()) {
            mappingStrategy.applyScanResults(exchange.getOut(), new HBaseData(scanOperationResult));
        }
    }

    private Put createPut(HBaseRow hRow) throws Exception {
        ObjectHelper.notNull(hRow, "HBase row");
        ObjectHelper.notNull(hRow.getId(), "HBase row id");
        ObjectHelper.notNull(hRow.getCells(), "HBase cells");

        Put put = new Put(HBaseHelper.toBytes(hRow.getId()));
        Set<HBaseCell> cells = hRow.getCells();
        for (HBaseCell cell : cells) {
            String family = cell.getFamily();
            String column = cell.getQualifier();
            Object value = cell.getValue();

            ObjectHelper.notNull(family, "HBase column family", cell);
            ObjectHelper.notNull(column, "HBase column", cell);
            put.add(HBaseHelper.getHBaseFieldAsBytes(family), HBaseHelper.getHBaseFieldAsBytes(column), HBaseHelper.toBytes(value));
        }
        return put;
    }

    private HBaseRow getCells(HTableInterface table, HBaseRow hRow) throws Exception {
        HBaseRow resultRow = new HBaseRow();
        List<HBaseCell> resultCells = new LinkedList<HBaseCell>();
        ObjectHelper.notNull(hRow, "HBase row");
        ObjectHelper.notNull(hRow.getId(), "HBase row id");
        ObjectHelper.notNull(hRow.getCells(), "HBase cells");

        resultRow.setId(hRow.getId());
        Get get = new Get(HBaseHelper.toBytes(hRow.getId()));
        Set<HBaseCell> cellModels = hRow.getCells();
        for (HBaseCell cellModel : cellModels) {
            String family = cellModel.getFamily();
            String column = cellModel.getQualifier();

            ObjectHelper.notNull(family, "HBase column family", cellModel);
            ObjectHelper.notNull(column, "HBase column", cellModel);
            get.addColumn(HBaseHelper.getHBaseFieldAsBytes(family), HBaseHelper.getHBaseFieldAsBytes(column));
        }

        Result result = table.get(get);

        for (HBaseCell cellModel : cellModels) {
            HBaseCell resultCell = new HBaseCell();
            String family = cellModel.getFamily();
            String column = cellModel.getQualifier();
            resultCell.setFamily(family);
            resultCell.setQualifier(column);

            List<KeyValue> kvs = result.getColumn(HBaseHelper.getHBaseFieldAsBytes(family), HBaseHelper.getHBaseFieldAsBytes(column));
            if (kvs != null && !kvs.isEmpty()) {
                //Return the most recent entry.
                // TODO double check conversion
                resultCell.setValue(Bytes.toString(kvs.get(0).getValue()));
            }
            resultCells.add(resultCell);
            resultRow.getCells().add(resultCell);
        }
        return resultRow;
    }

    private Delete createDeleteRow(HBaseRow hRow) throws Exception {
        ObjectHelper.notNull(hRow, "HBase row");
        ObjectHelper.notNull(hRow.getId(), "HBase row id");
        return new Delete(HBaseHelper.toBytes(hRow.getId()));
    }

    /**
    private List<HBaseRow> scanCells(HTableInterface table, HBaseRow model, List<Filter> filters) throws Exception {
        List<HBaseRow> rowSet = new LinkedList<HBaseRow>();
        Scan scan = new Scan();
        if (filters != null && !filters.isEmpty()) {
            for (Filter filter : filters) {
                if (ModelAwareFilter.class.isAssignableFrom(filter.getClass())) {
                    ((ModelAwareFilter<?>) filter).apply(endpoint.getCamelContext(), model);
                }
            }
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters));
        }
        Set<HBaseCell> cellModels = model.getCells();
        for (HBaseCell cellModel : cellModels) {
            String family = cellModel.getFamily();
            String column = cellModel.getQualifier();

            if (ObjectHelper.isNotEmpty(family) && ObjectHelper.isNotEmpty(column)) {
                scan.addColumn(HBaseHelper.getHBaseFieldAsBytes(family), HBaseHelper.getHBaseFieldAsBytes(column));
            }
        }

        ResultScanner resultScanner = table.getScanner(scan);
        Result result = resultScanner.next();
        while (result != null) {
            HBaseRow resultRow = new HBaseRow();
            resultRow.setId(endpoint.getCamelContext().getTypeConverter().convertTo(model.getRowType(), result.getRow()));
            cellModels = model.getCells();
            for (HBaseCell modelCell : cellModels) {
                HBaseCell resultCell = new HBaseCell();
                String family = modelCell.getFamily();
                String column = modelCell.getQualifier();
                resultRow.setId(endpoint.getCamelContext().getTypeConverter().convertTo(model.getRowType(), result.getRow()));
                resultCell.setValue(endpoint.getCamelContext().getTypeConverter().convertTo(modelCell.getValueType(),
                        result.getValue(HBaseHelper.getHBaseFieldAsBytes(family), HBaseHelper.getHBaseFieldAsBytes(column))));
                resultCell.setFamily(modelCell.getFamily());
                resultCell.setQualifier(modelCell.getQualifier());
                resultRow.getCells().add(resultCell);
                rowSet.add(resultRow);
            }

            result = resultScanner.next();
        }
        return rowSet;
    }
     */

}
