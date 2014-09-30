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

package com.stratio.connector.deep.mappings.transformation;

import java.math.BigInteger;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import com.stratio.connector.deep.mappings.structures.GroupBy;
import com.stratio.connector.deep.mappings.trasfer.ColumnInfo;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.meta.common.statements.structures.selectors.GroupByFunction;

import scala.Tuple2;

public class GroupByMapping implements PairFunction<Cells, Cells, Cells> {
    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -2763959543919248527L;
    private List<ColumnInfo> aggregationCols;
    private List<GroupBy> groupByClause;


    public GroupByMapping(List<ColumnInfo> aggregationCols, List<GroupBy> groupByClause) {
        this.aggregationCols = aggregationCols;
        this.groupByClause = groupByClause;
    }
    @Override
    public Tuple2<Cells, Cells> call(Cells cells) throws Exception {
        Cells grouppingKeys = new Cells();
        Cells cellsExtended = cells;
        // Copying aggregation columns to not apply the function over the original data
        if (aggregationCols != null) {
            for (ColumnInfo aggCol : aggregationCols) {
                if (GroupByFunction.COUNT == aggCol.getAggregationFunction()) {
                    cellsExtended.add(aggCol.getTable(),
                            Cell.create(aggCol.getColumnName(), new BigInteger("1")));
                } else if (GroupByFunction.AVG == aggCol.getAggregationFunction()) {
                    Cell cellToCopy =
                            cells.getCellByName(aggCol.getTable(), aggCol.getField());
                    cellsExtended.add(aggCol.getTable(),
                            Cell.create(aggCol.getField() + "_count", new BigInteger("1")));
                    cellsExtended.add(aggCol.getTable(),
                            Cell.create(aggCol.getField() + "_sum", cellToCopy.getCellValue()));
                } else {
                    Cell cellToCopy =
                            cells.getCellByName(aggCol.getTable(), aggCol.getField());
                    cellsExtended.add(aggCol.getTable(),
                            Cell.create(aggCol.getColumnName(), cellToCopy.getCellValue()));
                }
            }
        }
        if (groupByClause != null) {
            for (GroupBy groupByCol : groupByClause) {
                Cell cell=cells.getCellByName(groupByCol.getSelectorIdentifier().getTable(), groupByCol
                                .getSelectorIdentifier().getField());
                grouppingKeys.add(cell);
            }
        } else {
            grouppingKeys.add(Cell.create("_", "_"));
        }
        return new Tuple2<>(grouppingKeys, cellsExtended);
    }
}