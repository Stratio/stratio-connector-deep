/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.deep.engine.query.structures;

import java.io.Serializable;

import org.apache.log4j.Logger;

public abstract class Term<T extends Comparable<T>> extends ValueCell<T> implements Comparable<T>,
        Serializable {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(Term.class);

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

        if (!(this.clazz.isInstance(o))) {

            try {
                if ((this.clazz).equals(Long.class) && o.getClass().equals(Integer.class)) {
                    Long obj = Long.valueOf(o.toString());
                    return Long.valueOf(this.getStringValue()).compareTo(obj);
                } else if ((this.clazz).equals(Double.class) && o.getClass().equals(Float.class)) {
                    Double obj = Double.valueOf(o.toString());
                    return Double.valueOf(this.getStringValue()).compareTo(obj);
                }
            } catch (NumberFormatException | ClassCastException e) {
                LOG.error("Sorry, unable to Cast incompatible types ->" + this.clazz + " & " + o.getClass());
            }
            return this.value.compareTo(o);
        }
        return this.value.compareTo(o);
    }

    /**
     * Returns a hash code value for the object. This method is supported for the benefit of hash tables such as those
     * provided by {@link java.util.HashMap}.
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
     * @param obj
     *            the reference object with which to compare.
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
            try {
                if (obj instanceof String) {
                    return this.getStringValue().equals(obj);
                } else if (obj instanceof Float) {
                    return Float.valueOf(this.getStringValue()).equals(obj);
                } else if (obj instanceof Integer) {
                    return Integer.valueOf(this.getStringValue()).equals(obj);
                } else if (obj instanceof Long) {
                    return Long.valueOf(this.getStringValue()).equals(obj);
                }
            } catch (NumberFormatException e) {
                LOG.error("Sorry, unable to Cast incompatible types ->" + this.clazz + " & " + obj.getClass());
            }
            return super.equals(obj);
        }
        return this.value.equals(obj);
    }
}