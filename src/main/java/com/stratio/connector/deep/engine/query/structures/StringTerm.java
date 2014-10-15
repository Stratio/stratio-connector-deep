package com.stratio.connector.deep.engine.query.structures;


public class StringTerm extends Term<String> {
    private static final long serialVersionUID = 4470491967411363431L;
    private boolean quotedLiteral = false;
    public StringTerm(String term, boolean quotedLiteral) {
        super(String.class, term);
        this.type = TYPE_TERM;
        this.quotedLiteral = quotedLiteral;
    }
    public StringTerm(String term) {
        this(term, false);
    }
    public StringTerm(Term<String> term) {
        super(String.class, term.getTermValue().toString());
    }
    public boolean isQuotedLiteral() {
        return quotedLiteral;
    }
    /** {@inheritDoc} */
    @Override
    public String toString() {
        if (this.isQuotedLiteral()) {
            return "'" + value + "'";
        } else {
            return value;
        }
    }
}
