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

import org.apache.spark.api.java.function.Function;

import com.stratio.connector.deep.engine.query.structures.Term;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.deep.commons.entity.Cells;

/**
 * Class that defines LessThan, Spark function that determines if the value in the given field is less than the provided term.
 */
public class LessThan implements Function<Cells, Boolean> {

	private static final long serialVersionUID = 2675616112608139116L;

	/**
	 * Term to be compared.
	 */
	private final Term<?> term;

	/**
	 * Column cell to compare to.
	 */
	private final ColumnName column;

	/**
	 * Basic constructor for the LessThan function class.
	 * 
	 * @param column
	 * 				Column cell to compare to
	 * @param term
	 * 				Term to be compared
	 */
	public LessThan(ColumnName column, Term term) {
		this.term = term;
		this.column = column;
	}

	@Override
	public Boolean call(Cells cells) {
		Object obj = cells.getCellByName(column.getTableName().getQualifiedName(), column.getName()).getCellValue();
		return ((Comparable) term).compareTo(obj) > 0;
	}
}
