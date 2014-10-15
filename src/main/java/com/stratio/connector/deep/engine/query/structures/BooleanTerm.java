package com.stratio.connector.deep.engine.query.structures;

public class BooleanTerm extends Term<Boolean> {
    private static final long serialVersionUID = 2872212148572680680L;
    /**
     * Class constructor.
     *
     * @param term
     * The string representation of a Boolean value.
     */
    public BooleanTerm(String term) {
        super(Boolean.class, Boolean.valueOf(term));
    }
    public BooleanTerm(Term<Boolean> term) {
        super(Boolean.class, term.getTermValue().booleanValue());
    }
}