package com.stratio.connector.deep.engine.query.structures;

import java.io.Serializable;


public abstract class Term<T extends Comparable<T>> extends ValueCell<T> implements Comparable<T>,
        Serializable {
    private static final long serialVersionUID = -4258938152892510227L;
    protected Class<? extends Comparable<?>> clazz;
    protected T value;

    public Term(Class<? extends Comparable<?>> clazz, T value) {
        this.clazz = clazz;
        this.value = value;
    }
    /**
     * Get the Term Java Class.
     *
     * @return A {@link java.lang.Class}.
     */
    public Class<? extends Comparable<?>> getTermClass() {
        return clazz;
    }
    /**
     * Get the term value.
     *
     * @return A {@link java.lang.Object} with the value.
     */
    public T getTermValue() {
        return value;
    }
    @Override
    public String getStringValue() {
        return value.toString();
    }
    @Override
    public String toString() {
        return value.toString();
    }
    /*
    * (non-Javadoc)
    *
    * @see java.lang.Comparable#compareTo(java.lang.Object)
    */
    @Override
    public int compareTo(T o) {
        return this.value.compareTo(o);
    }
    /**
     * Returns a hash code value for the object. This method is supported for the benefit of hash
     * tables such as those provided by {@link java.util.HashMap}.
     *
     * @return a hash code value for this object.
     * @see Object#equals(Object)
     * @see System#identityHashCode
     */
    @Override
    public int hashCode() {
        return clazz.hashCode() * this.getTermValue().hashCode();
    }
    /**
     * Indicates whether some other object is "equal to" this one.
     * <p/>
     * The {@code equals} method implements an equivalence relation on non-null object references:
     *
     * @param obj the reference object with which to compare.
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise.
     * @see #hashCode()
     * @see java.util.HashMap
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(this.clazz.isInstance(obj))) {
            if(obj instanceof String){
                String value = this.getStringValue();
                return value.equals(obj);
            }else if(obj instanceof Float){
                Float value = Float.valueOf(this.getStringValue());
                return value.equals(obj);
            }
            return super.equals(obj);
        }
        return this.value.equals((T) obj);
    }
}