package com.stratio.connector.deep.engine.query.structures;

public class FloatTerm extends Term<Float> {
    private static final long serialVersionUID = -578510540271635667L;
    public FloatTerm(String term) {
        super(Float.class, Float.valueOf(term));
    }
    public FloatTerm(Float term) {
        super(Float.class, term);
    }
    public FloatTerm(Term<Double> term) {
        super(Float.class, term.getTermValue().floatValue());
    }
}