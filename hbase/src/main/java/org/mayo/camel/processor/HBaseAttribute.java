package org.mayo.camel.processor;

public enum HBaseAttribute {

    HBASE_ROW_ID("CamelHBaseRowId"),
    HBASE_ROW_TYPE("CamelHBaseRowType"),
    HBASE_MARKED_ROW_ID("CamelHBaseMarkedRowId"),
    HBASE_FAMILY("CamelHBaseFamily"),
    HBASE_QUALIFIER("CamelHBaseQualifier"),
    HBASE_VALUE("CamelHBaseValue"),
    HBASE_VALUE_TYPE("CamelHBaseValueType");

    private final String value;

    private HBaseAttribute(String value) {
        this.value = value;
    }

    public String asHeader(int i) {
        if (i > 1) {
            return value + i;
        } else {
            return value;
        }
    }

    public String asHeader() {
        return value;
    }

    public String asOption() {
        String normalizedValue = value.replaceAll("CamelHBase", "");
        return normalizedValue.substring(0, 1).toLowerCase() + normalizedValue.substring(1);
    }

    public String asOption(int i) {
        String option = asOption();
        if (i > 1) {
            return option + i;
        } else {
            return option;
        }
    }

    @Override
    public String toString() {
        return value;
    }

}
