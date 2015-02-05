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

/**
 * Class that defines In, Spark function that determines if the value in the given field is included in the provided list of terms.
 */
public class In implements Function<Cells, Boolean> {

	private static final long serialVersionUID = -6637139616271541577L;

	/**
	 * Name of the field of the cell to be compared.
	 */
	private final String field;

	/**
	 * IDs in the IN clause.
	 */
	private final List<Serializable> terms;

	/**
	 * Basic constructor for the In function class.
	 * @param field
	 * 				Name of the field of the cell to be compared
	 * @param terms
	 * 				IDs in the IN clause
	 */
	public In(String field, List<Serializable> terms) {
		this.field = field;
		this.terms = terms;
	}

	@Override
	public Boolean call(Cells cells) {

		Boolean isValid = false;

		if (this.field != null && this.terms != null) {
			// TODO: Implements call to create IN stuff
		}

		return isValid;
	}
}
