package org.mayo.camel.processor.mapping;

import org.apache.camel.Message;
import org.mayo.camel.processor.HBaseAttribute;
import org.mayo.camel.processor.model.HBaseCell;
import org.mayo.camel.processor.model.HBaseData;
import org.mayo.camel.processor.model.HBaseRow;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class HeaderMappingStrategy {

    private HBaseRow resolveRow(Message message, int index) {
        HBaseRow hRow = new HBaseRow();
        HBaseCell hCell = new HBaseCell();

        if (message != null) {
            Object id =  message.getHeader(HBaseAttribute.HBASE_ROW_ID.asHeader(index));
            String rowClassName = message.getHeader(HBaseAttribute.HBASE_ROW_TYPE.asHeader(index), String.class);
            Class<?> rowClass = rowClassName == null || rowClassName.isEmpty() ? String.class : message.getExchange().getContext().getClassResolver().resolveClass(rowClassName);
            String columnFamily = (String) message.getHeader(HBaseAttribute.HBASE_FAMILY.asHeader(index));
            String columnName = (String) message.getHeader(HBaseAttribute.HBASE_QUALIFIER.asHeader(index));
            Object value =  message.getHeader(HBaseAttribute.HBASE_VALUE.asHeader(index));

            String valueClassName = message.getHeader(HBaseAttribute.HBASE_VALUE_TYPE.asHeader(index), String.class);
            Class<?> valueClass = valueClassName == null || valueClassName.isEmpty() ? String.class : message.getExchange().getContext().getClassResolver().resolveClass(valueClassName);

            //Id can be accepted as null when using get, scan etc.
            if (id == null && columnFamily == null && columnName == null) {
                return null;
            }

            hRow.setId(id);
            hRow.setRowType(rowClass);
            if (columnFamily != null && columnName != null) {
                hCell.setQualifier(columnName);
                hCell.setFamily(columnFamily);
                hCell.setValue(value);
                // String is the default value type
                hCell.setValueType((valueClass != null) ? valueClass : String.class);
                hRow.getCells().add(hCell);
            }
        }
        return hRow;
    }

    public HBaseData resolveModel(Message message) {
        int index = 1;
        HBaseData data = new HBaseData();
        //We use a LinkedHashMap to preserve the order.
        Map<Object, HBaseRow> rows = new LinkedHashMap<Object, HBaseRow>();
        HBaseRow hRow = new HBaseRow();
        while (hRow != null) {
            hRow = resolveRow(message, index++);
            if (hRow != null) {
                if (rows.containsKey(hRow.getId())) {
                    rows.get(hRow.getId()).getCells().addAll(hRow.getCells());
                } else {
                    rows.put(hRow.getId(), hRow);
                }
            }
        }
        for (Map.Entry<Object, HBaseRow> rowEntry : rows.entrySet()) {
            data.getRows().add(rowEntry.getValue());
        }
        return data;
    }

    public void applyGetResults(Message message, HBaseData data) {
        int index = 1;
        if (data == null || data.getRows() == null) {
            return;
        }

        for (HBaseRow hRow : data.getRows()) {
            if (hRow.getId() != null) {
                Set<HBaseCell> cells = hRow.getCells();
                for (HBaseCell cell : cells) {
                    message.setHeader(HBaseAttribute.HBASE_VALUE.asHeader(index++), getValueForColumn(cells, cell.getFamily(), cell.getQualifier()));
                }
            }
        }
    }

    public void applyScanResults(Message message, HBaseData data) {
        int index = 1;
        if (data == null || data.getRows() == null) {
            return;
        }

        for (HBaseRow hRow : data.getRows()) {
            Set<HBaseCell> cells = hRow.getCells();
            for (HBaseCell cell : cells) {
                message.setHeader(HBaseAttribute.HBASE_ROW_ID.asHeader(index), hRow.getId());
                message.setHeader(HBaseAttribute.HBASE_FAMILY.asHeader(index), cell.getFamily());
                message.setHeader(HBaseAttribute.HBASE_QUALIFIER.asHeader(index), cell.getQualifier());
                message.setHeader(HBaseAttribute.HBASE_VALUE.asHeader(index), cell.getValue());
            }
            index++;
        }
    }

    private Object getValueForColumn(Set<HBaseCell> cells, String family, String qualifier) {
        if (cells != null) {
            for (HBaseCell cell : cells) {
                if (cell.getQualifier().equals(qualifier) && cell.getFamily().equals(family)) {
                    return cell.getValue();
                }
            }
        }
        return null;
    }

}
