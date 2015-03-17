package org.mayo.camel.processor.model;

import javax.xml.bind.annotation.XmlAttribute;

public class HBaseCell {

    private String family;
    private String qualifier;
    private Object value;
    private Class<?> valueType = String.class;

    public String toString() {
        return "HBaseCell=[family=" + family + ", qualifier=" + qualifier + ", value=" + value + ", valueType=" + valueType.getName() + "]";
    }

    @XmlAttribute(name = "family")
    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    @XmlAttribute(name = "qualifier")
    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @XmlAttribute(name = "type")
    public Class<?> getValueType() {
        return valueType;
    }

    public void setValueType(Class<?> valueType) {
        if (valueType == null) {
            throw new IllegalArgumentException("Value type can not be null");
        }
        this.valueType = valueType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HBaseCell cell = (HBaseCell) o;

        if (family != null ? !family.equals(cell.family) : cell.family != null) {
            return false;
        }
        if (qualifier != null ? !qualifier.equals(cell.qualifier) : cell.qualifier != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = family != null ? family.hashCode() : 0;
        result = 31 * result + (qualifier != null ? qualifier.hashCode() : 0);
        return result;
    }


}
