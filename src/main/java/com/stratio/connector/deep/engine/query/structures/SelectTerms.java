package com.stratio.connector.deep.engine.query.structures;

import java.io.Serializable;

import com.stratio.deep.commons.filter.Filter;


public class SelectTerms {

    private String field;

    private String operation;

    private Serializable value;


    public SelectTerms(String field){
        this.field = field;
    }

    public SelectTerms(String field, String operation, Serializable value){
        this.field = field;
        this.operation = operation;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Serializable getValue() {
        return value;
    }

    public void setValue(Serializable value) {
        this.value = value;
    }
}
