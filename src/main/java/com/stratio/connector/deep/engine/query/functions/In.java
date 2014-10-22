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

package com.stratio.connector.deep.engine.query.functions;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.commons.entity.Cells;

public class In implements Function<Cells, Boolean> {

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -6637139616271541577L;

    /**
     * Name of the field of the cell to compare.
     */
    private final String field;

    /**
     * IDs in the IN clause.
     */
    private final List<Serializable> terms;

    /**
     * In apply in filter to a field in a Deep Cell.
     * 
     * @param field
     *            Name of the field to check.
     * @param terms
     *            List of terms of the IN clause.
     */
    public In(String field, List<Serializable> terms) {
        this.field = field;
        this.terms = terms;
    }

    @Override
    public Boolean call(Cells cells) {

        Boolean isValid = false;

        // TODO: Implements call to create IN stuff

        return isValid;
    }

    private Boolean isIncludedInList(List<Serializable> list, Object value) {
        // TODO: Implements call to create IN stuff

        return false;
    }
}
