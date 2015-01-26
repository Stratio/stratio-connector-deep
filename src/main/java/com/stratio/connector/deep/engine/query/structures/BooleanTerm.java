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

/**
 * 
 * Subclass of the class 'Term' that defines a Boolean Term.
 *
 */
public class BooleanTerm extends Term<Boolean> {
    private static final long serialVersionUID = 2872212148572680680L;

    /**
     * Constructor from a String representation of a Boolean value.
     * 
     * @param term
     *            The string representation of a Boolean value
     */
    public BooleanTerm(String term) {
        super(Boolean.class, Boolean.valueOf(term));
    }
}