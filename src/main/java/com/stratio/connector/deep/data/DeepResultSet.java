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

package com.stratio.connector.deep.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.metadata.ColumnMetadata;

/**
 * 
 * Subclass DeepResultSet that extends ResultSet and implements Serializable.
 *
 */
public class DeepResultSet extends ResultSet implements Serializable {

	/**
	 * Serial version UID in order to be Serializable.
	 */
	private static final long serialVersionUID = 7054678344109801821L;

	/**
	 * List of {@link com.stratio.crossdata.common.data.Row}.
	 */
	private List<Row> rows;

	/**
	 * List of {@link ColumnMetadata}.
	 */
	private List<ColumnMetadata> columnMetadata;

	/**
	 * CassandraResultSet default constructor.
	 */
	public DeepResultSet() {
		rows = new ArrayList<>();
		columnMetadata = new ArrayList<>();
	}

	/**
	 * Set the list of rows.
	 * 
	 * @param rows
	 *            The list
	 */
	@Override
	public void setRows(List<Row> rows) {
		this.rows = rows;
	}

	/**
	 * Get the rows of the Result Set.
	 * 
	 * @return A List of {@link com.stratio.crossdata.common.data.Row}
	 */
	@Override
	public List<Row> getRows() {
		return rows;
	}

	/**
	 * Set the list of column metadata.
	 * 
	 * @param columnMetadata
	 *            A list of {@link ColumnMetadata} in order
	 */
	@Override
	public void setColumnMetadata(List<ColumnMetadata> columnMetadata) {
		this.columnMetadata = columnMetadata;
	}

	/**
	 * Get the column metadata in order.
	 * 
	 * @return A list of {@link ColumnMetadata}
	 */
	@Override
	public List<ColumnMetadata> getColumnMetadata() {
		return columnMetadata;
	}

	/**
	 * Add a row to the Result Set.
	 * 
	 * @param row
	 *            {@link com.stratio.crossdata.common.data.Row} to add
	 */
	@Override
	public void add(Row row) {
		rows.add(row);
	}

	/**
	 * Remove a row.
	 * 
	 * @param index
	 *            Index of the row to remove
	 */
	@Override
	public void remove(int index) {
		rows.remove(index);
	}

	/**
	 * Get the size of the Result Set.
	 * 
	 * @return 
	 * 			The size
	 */
	@Override
	public int size() {
		return rows.size();
	}

	@Override
	public Iterator<Row> iterator() {
		return new DeepResultSetIterator(this);
	}

}
