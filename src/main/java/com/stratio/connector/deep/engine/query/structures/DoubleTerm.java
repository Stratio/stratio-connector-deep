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
 * Subclass of the class 'Term' that defines a Double Term.
 *
 */
public class DoubleTerm extends Term<Double> {

	private static final long serialVersionUID = -578510540271635667L;

	/**
	 * Constructor from a String representation of a Double value.
	 * @param term
	 * 				String representation of a Double value
	 */
	public DoubleTerm(String term) {
		super(Double.class, Double.valueOf(term));
	}

	/**
	 * Constructor from Double type.
	 * @param term
	 * 				Double value
	 */
	public DoubleTerm(Double term) {
		super(Double.class, term);
	}
}