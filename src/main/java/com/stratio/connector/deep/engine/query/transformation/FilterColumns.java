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

import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;

/**
 * Class that defines FilterColumns, Spark function that filters requested columns from a given {@link Cells} object.
 */
public class FilterColumns implements Function<Cells, Cells> {

    private static final long serialVersionUID = -6143471789450703044L;

    /**
     * Columns to be kept
     */
    private final List<Selector> columns;

    /**
     * Basic constructor.
     * 
     * @param columns
     * 				The columns
     */
    public FilterColumns(List<Selector> columns) {
        this.columns = columns;
    }

    @Override
    public Cells call(Cells cells) {

        Cells cellsOut = new Cells();
        for (Selector columnName : columns) {
            Cell cell = cells.getCellByName(columnName.getColumnName().getTableName().getQualifiedName(),
                    columnName.getColumnName().getName());
            cellsOut.add(columnName.getColumnName().getTableName().getQualifiedName(), cell);
        }

        return cellsOut;
    }
}
