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

package com.stratio.connector.deep.engine.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Join;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.PartialResults;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * @author david
 * 
 */
public final class QueryPartialResultsUtils {

    private static final int NUMBER_OF_ELEMENTS_IN_QUALIFIED_COLUMN_NAME = 3;

    private QueryPartialResultsUtils() {
    }

    /**
     * @param resultSet
     * @return
     * @throws ExecutionException
     */
    public static List<Cells> createCellsFromResultSet(ResultSet resultSet) throws ExecutionException {
        List<Row> rows = resultSet.getRows();
        List<Cells> cellsList = null;
        if (rows != null && rows.size() > 0) {
            cellsList = new ArrayList<Cells>(rows.size());
            if (cellsList != null) {
                List<ColumnMetadata> columnsMetadata = resultSet.getColumnMetadata();
                String qualifiedName = columnsMetadata.get(0).getTableName();
                String[] arrNames = qualifiedName.split("\\.");
                if (arrNames.length != 2) {
                    throw new ExecutionException(
                            "Table name must be a qualified name: [catalog_name.table_name] but is: "
                                    + columnsMetadata.get(0).getTableName());
                }
                String catalogName = arrNames[0];
                String tableName = arrNames[1];
                for (Row row : rows) {
                    cellsList.add(buildCellsFromRow(row, catalogName, tableName, columnsMetadata));
                }
            }
        }
        return cellsList;
    }

    /**
     * @param row
     * @param catalogName
     * @param tableName
     * @param columnsMetadata
     * @return
     * @throws ExecutionException
     */
    public static Cells buildCellsFromRow(Row row, String catalogName, String tableName,
            List<ColumnMetadata> columnsMetadata) throws ExecutionException {
        Cells cells = new Cells(catalogName + "." + tableName);
        Map<String, String> aliasMapping = new HashMap<String, String>();
        for (ColumnMetadata colMetadata : columnsMetadata) {
            aliasMapping.put(colMetadata.getColumnAlias(),
                    getColumnNameFromQualifiedColumnName(colMetadata.getColumnName()));

        }

        for (Entry<String, Cell> colItem : row.getCells().entrySet()) {
            String cellName = aliasMapping.containsKey(colItem.getKey()) ? aliasMapping.get(colItem.getKey()) : colItem
                    .getKey();
            cells.add(com.stratio.deep.commons.entity.Cell.create(cellName, colItem.getValue().getValue()));
        }
        return cells;
    }

    /**
     * @param deepContext
     * @param resSet
     * @return
     * @throws ExecutionException
     */
    public static JavaRDD<Cells> createRDDFromResultSet(DeepSparkContext deepContext, ResultSet resSet)
            throws ExecutionException {
        List<Cells> cellsList = createCellsFromResultSet(resSet);
        if (cellsList == null) {
            throw new ExecutionException("An empty result set is not allowed in a join with partial results");
        }

        return deepContext.parallelize(cellsList);
    }

    /**
     * @param joinStep
     * @return
     * @throws UnsupportedException
     */
    public static PartialResults getPartialResult(Join joinStep) throws UnsupportedException {
        Iterator<LogicalStep> iterator = joinStep.getPreviousSteps().iterator();
        PartialResults partialResults = null;
        while (iterator.hasNext() && partialResults == null) {
            LogicalStep lStep = iterator.next();
            if (lStep instanceof PartialResults) {
                partialResults = (PartialResults) lStep;
            }
        }
        validatePartialResult(partialResults);

        return partialResults;
    }

    /**
     * @param partialResults
     * @throws UnsupportedException
     */
    public static void validatePartialResult(PartialResults partialResults) throws UnsupportedException {
        if (partialResults == null) {
            throw new UnsupportedException("Missing logical step \"partialResults\" in a join with partial results");
        }
        if (partialResults.getResults() == null) {
            throw new UnsupportedException("Missing result set in the partial result");
        }
        if (partialResults.getResults().getColumnMetadata() == null) {
            throw new UnsupportedException("Missing column metadata in the partial result");
        }

    }

    /**
     * Returns a rearrange list of relations. Assign each "partialResults" selector to the left part of the relation
     * 
     * @param partialResults
     * @param joinRelations
     * @return
     */
    public static List<Relation> getOrderedRelations(PartialResults partialResults, List<Relation> joinRelations) {
        List<Relation> orderedRelations = new ArrayList<Relation>();
        for (Relation relation : joinRelations) {
            ColumnSelector colSelector = (ColumnSelector) relation.getLeftTerm();
            String partialResultsQualifiedTableName = partialResults.getResults().getColumnMetadata().get(0)
                    .getTableName();
            if (colSelector.getName().getTableName().getQualifiedName().equals(partialResultsQualifiedTableName)) {
                orderedRelations.add(relation);
            } else {
                orderedRelations.add(new Relation(relation.getRightTerm(), relation.getOperator(), relation
                        .getLeftTerm()));
            }
        }
        return orderedRelations;
    }

    /**
     * @param qualifiedName
     * @return
     * @throws ExecutionException
     */
    public static String getColumnNameFromQualifiedColumnName(String qualifiedName) throws ExecutionException {

        if (qualifiedName != null && !qualifiedName.trim().isEmpty()) {
            String[] arrNames = qualifiedName.split("\\.");

            if (arrNames.length == NUMBER_OF_ELEMENTS_IN_QUALIFIED_COLUMN_NAME) {
                return arrNames[2];
            }
        }

        throw new ExecutionException("Qualified column name is expected instead of : " + qualifiedName);

    }
}
