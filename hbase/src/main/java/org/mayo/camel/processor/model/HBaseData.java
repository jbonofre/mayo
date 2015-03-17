package org.mayo.camel.processor.model;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement(name = "data")
public class HBaseData {

    private List<HBaseRow> rows = new LinkedList<HBaseRow>();

    public HBaseData() {
    }

    public HBaseData(List<HBaseRow> rows) {
        this.rows = rows;
    }

    public List<HBaseRow> getRows() {
        return rows;
    }

    public void setRows(List<HBaseRow> rows) {
        this.rows = rows;
    }


}
