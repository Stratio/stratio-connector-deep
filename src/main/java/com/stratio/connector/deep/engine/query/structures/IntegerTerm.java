package com.stratio.connector.deep.engine.query.structures;

public class IntegerTerm extends Term<Integer> {

    private static final long serialVersionUID = 7097178218828822792L;
    public IntegerTerm(String term) {
        super(Integer.class, Integer.valueOf(term));
    }
    public IntegerTerm(Integer term) {
        super(Integer.class, term);
    }
    public IntegerTerm(Term<Long> term) {
        super(Integer.class, term.getTermValue().intValue());
    }
}