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

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.commons.entity.Cells;

/**
 * Spark function that determines if the value in the given field is between the provided lower and upper bounds.
 */
public class Between implements Function<Cells, Boolean> {

	private static final long serialVersionUID = -4498262312538738011L;

	/**
	 * Name of the field of the cell to compare.
	 */
	private final String field;

	/**
	 * Lower bound.
	 */
	private final Serializable lowerBound;

	/**
	 * Upper bound.
	 */
	private final Serializable upperBound;

	/**
	 * Basic constructor for the Between function class.
	 * 
	 * @param field
	 * 				Name of the field of the cell to compare
	 * @param lowerBound
	 * 					 The lower bound	
	 * @param upperBound
	 * 					 The upper bound
	 */
	public Between(String field, Serializable lowerBound, Serializable upperBound) {
		this.field = field;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Boolean call(Cells cells) {

		Boolean isValid = false;
		Object cellValue = cells.getCellByName(field).getCellValue();

		if (cellValue != null) {
			isValid =
					(((Comparable) lowerBound).compareTo(cellValue) <= 0)
					&& (((Comparable) upperBound).compareTo(cellValue) >= 0);
		}

		return isValid;
	}
}
