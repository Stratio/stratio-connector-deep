package com.stratio.connector.deep.engine.query.structures;

/**
 * Created by dgomez on 3/10/14.
 */
public abstract class ValueCell<T extends Comparable<T>> {

    public static final int TYPE_TERM = 1;
    public static final int TYPE_COLLECTION_LITERAL = 2;

    protected int type;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
    /**
     * Get the String value representation.
     *
     * @return The String value.
     */
    public abstract String getStringValue();
    @Override
    public abstract String toString();
}


