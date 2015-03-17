package org.mayo.camel.processor.model;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedHashSet;
import java.util.Set;

// TODO instead of using JAXB and go with XML, we can directly use Jackson to create JSON !!!!

@XmlRootElement(name = "row")
public class HBaseRow implements Cloneable {

    private Object id;
    private Class<?> rowType = String.class;
    private Set<HBaseCell> cells;

    public HBaseRow() {
        this(new LinkedHashSet<HBaseCell>());
    }

    public HBaseRow(Set<HBaseCell> cells) {
        this.cells = cells;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    @XmlAttribute(name = "type")
    public Class<?> getRowType() {
        return rowType;
    }

    public void setRowType(Class<?> rowType) {
        this.rowType = rowType;
    }

    public Set<HBaseCell> getCells() {
        return cells;
    }

    public void setCells(Set<HBaseCell> cells) {
        this.cells = cells;
    }

    public boolean isEmpty() {
        return cells.isEmpty();
    }

    public int size() {
        return cells.size();
    }

    public void apply(HBaseRow modelRow) {
        if (modelRow != null) {
            if (rowType == null && modelRow.getRowType() != null) {
                rowType = modelRow.getRowType();
            }
            for (HBaseCell modelCell : modelRow.getCells()) {
                if (!getCells().contains(modelCell)) {
                    HBaseCell cell = new HBaseCell();
                    cell.setFamily(modelCell.getFamily());
                    cell.setQualifier(modelCell.getQualifier());
                    cell.setValueType(modelCell.getValueType());
                    getCells().add(cell);
                }
            }
        }
    }

}
