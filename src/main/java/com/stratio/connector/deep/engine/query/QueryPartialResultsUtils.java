/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.connector.deep.engine.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.PartialResults;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;

/**
 * @author david
 *
 */
public class QueryPartialResultsUtils {

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
                String qualifiedName = resultSet.getColumnMetadata().get(0).getTableName();
                String[] arrNames = qualifiedName.split("\\.");
                if (arrNames.length != 2)
                    throw new ExecutionException(
                                    "Table name must be a qualified name: [catalog_name.table_name] but is: "
                                                    + resultSet.getColumnMetadata().get(0).getTableName());
                String catalogName = arrNames[0];
                String tableName = arrNames[1];
                for (Row row : rows) {
                    cellsList.add(buildCellsFromRow(row, catalogName, tableName));
                }
            }
        }
        return cellsList;
    }

    /**
     * @param row
     * @param catalogName
     * @param tableName
     * @return
     */
    public static Cells buildCellsFromRow(Row row, String catalogName, String tableName) {
        Cells cells = new Cells(catalogName + "." + tableName);
        for (Entry<String, Cell> colItem : row.getCells().entrySet()) {
            cells.add(com.stratio.deep.commons.entity.Cell.create(colItem.getKey(), colItem.getValue().getValue()));
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
        JavaRDD<Cells> partialResultsRDD = deepContext.parallelize(cellsList);
        return partialResultsRDD;
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
}