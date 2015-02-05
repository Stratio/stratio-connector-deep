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
 * Class that defines a ValueCell.
 *
 * @param <T>
 * 				Type that extends Comparable<T>
 */
public abstract class ValueCell<T extends Comparable<T>> {
	public static final int TYPE_TERM = 1;
	public static final int TYPE_COLLECTION_LITERAL = 2;
	protected int type;

	/**
	 * Method that returns the type of a valueCell.
	 * 
	 * @return
	 * 			The type
	 */
	public int getType() {
		return type;
	}

	/**
	 * Method that sets the type in a valueCell.
	 * 
	 * @param type
	 * 				The type to be set
	 */
	public void setType(int type) {
		this.type = type;
	}

	/**
	 * Get the String value representation.
	 * 
	 * @return The String value
	 */
	public abstract String getStringValue();

	@Override
	public abstract String toString();
}