package com.stratio.connector.deep.engine.query.structures;

public class LongTerm extends Term<Long> {

    private static final long serialVersionUID = 7097178218828822792L;

    public LongTerm(String term) {
        super(Long.class, Long.valueOf(term));
    }

    public LongTerm(Long term) {
        super(Long.class, term);
    }

    public LongTerm(Term<Long> term) {
        super(Long.class, term.getTermValue().longValue());
    }
}




