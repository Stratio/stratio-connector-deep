package com.stratio.connector.deep.engine.query.structures;

public class DoubleTerm extends Term<Double> {

    private static final long serialVersionUID = -578510540271635667L;

    public DoubleTerm(String term) {
        super(Double.class, Double.valueOf(term));
    }
    public DoubleTerm(Double term) {
        super(Double.class, term);
    }

    public DoubleTerm(Term<Double> term) {
        super(Double.class, term.getTermValue().doubleValue());
    }
}