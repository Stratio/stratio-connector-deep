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

package com.stratio.connector.deep.engine.query.transformation;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;

/**
 * Spark pair function that builds a {@link Tuple2} composed by a key, which is the list of data contained in the keys
 * list, and the given {@link Cells} object.
 */
public class MapKeyForJoin implements PairFunction<Cells, List<Object>, Cells> {

    private static final long serialVersionUID = -6677647619149716567L;

    /**
     * Map key.
     */
    private final List<ColumnName> keys;

    /**
     * MapKeyForJoin maps a field in a Cell.
     * 
     * @param keys
     *            Field to map
     */
    public MapKeyForJoin(List<ColumnName> keys) {
        this.keys = keys;
    }

    @Override
    public Tuple2<List<Object>, Cells> call(Cells cells) {

        List<Object> cellsValues = new LinkedList<>();
        for (ColumnName columnKey : keys) {
            String tableName = columnKey.getTableName().getQualifiedName();
            Cell cell = cells.getCellByName(tableName, columnKey.getName());
            cellsValues.add(cell.getCellValue());
        }

        return new Tuple2<>(cellsValues, cells);
    }
}
