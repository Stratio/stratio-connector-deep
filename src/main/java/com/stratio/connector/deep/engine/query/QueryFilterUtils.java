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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.stratio.connector.deep.engine.query.functions.DeepEquals;
import com.stratio.connector.deep.engine.query.functions.GreaterEqualThan;
import com.stratio.connector.deep.engine.query.functions.GreaterThan;
import com.stratio.connector.deep.engine.query.functions.LessEqualThan;
import com.stratio.connector.deep.engine.query.functions.LessThan;
import com.stratio.connector.deep.engine.query.functions.NotEquals;
import com.stratio.connector.deep.engine.query.functions.OrderByComparator;
import com.stratio.connector.deep.engine.query.structures.BooleanTerm;
import com.stratio.connector.deep.engine.query.structures.DoubleTerm;
import com.stratio.connector.deep.engine.query.structures.LongTerm;
import com.stratio.connector.deep.engine.query.structures.StringTerm;
import com.stratio.connector.deep.engine.query.structures.Term;
import com.stratio.connector.deep.engine.query.transformation.FilterColumns;
import com.stratio.connector.deep.engine.query.transformation.JoinCells;
import com.stratio.connector.deep.engine.query.transformation.MapKeyForJoin;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FloatingPointSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.filter.FilterType;

import scala.Tuple2;

/**
 * Utils for the query Filters.
 */
public final class QueryFilterUtils {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(QueryFilterUtils.class);

    private QueryFilterUtils() {
    }

    /**
     * Take a RDD and a Relation and apply suitable filter to the RDD. Execute where clause on Deep.
     * 
     * @param rdd
     *            RDD which filter must be applied.
     * @param relation
     *            {@link com.stratio.crossdata.common.statements.structures.relationships.Relation} to apply.
     * @return A new RDD with the result.
     * @throws UnsupportedException
     */
    static JavaRDD<Cells> doWhere(JavaRDD<Cells> rdd, Relation relation) throws UnsupportedException,
            ExecutionException {

        Operator operator = relation.getOperator();
        JavaRDD<Cells> result = null;
        ColumnName column = ((ColumnSelector) relation.getLeftTerm()).getName();
        Term rightTerm = filterFromRightTermWhereRelation(relation);

        try {
            switch (operator) {
            case EQ:
                result = rdd.filter(new DeepEquals(column, rightTerm));
                break;
            case DISTINCT:
                result = rdd.filter(new NotEquals(column, rightTerm));
                break;
            case GT:
                result = rdd.filter(new GreaterThan(column, rightTerm));
                break;
            case GET:
                result = rdd.filter(new GreaterEqualThan(column, rightTerm));
                break;
            case LT:
                result = rdd.filter(new LessThan(column, rightTerm));
                break;
            case LET:
                result = rdd.filter(new LessEqualThan(column, rightTerm));
                break;
            case IN:

                throw new UnsupportedException("IN operator unsupported");
            case BETWEEN:

                throw new UnsupportedException("BETWEEN operator unsupported");
            default:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Operator not supported: " + operator);
                }

                result = null;
            }
        } catch (Exception e) {
            throw new ExecutionException(" Error when try to comparate fields[ ]" + e);
        }
        return result;
    }

    /**
     * Build JavaRDD<Cells> from list of Cells and select Columns.
     * 
     * @param rdd
     *            Cells RDD.
     * @param selectedCols
     *            Set of fields selected in the SelectStatement.
     * @return JavaRDD<Cells>
     */

    static JavaRDD<Cells> filterSelectedColumns(JavaRDD<Cells> rdd, final Set<Selector> selectedCols) {

        List<Selector> list = new ArrayList<>(selectedCols);

        return rdd.map(new FilterColumns(list));
    }

    static JavaRDD<Cells> doJoin(JavaRDD<Cells> leftRdd, JavaRDD<Cells> rightRdd, List<Relation> joinRelations) {

        JavaRDD<Cells> joinedResult = null;

        List<ColumnName> firstTables = new ArrayList<>();
        List<ColumnName> secondTables = new ArrayList<>();

        for (Relation relation : joinRelations) {

            ColumnSelector selectorRight = (ColumnSelector) relation.getRightTerm();
            ColumnSelector selectorLeft = (ColumnSelector) relation.getLeftTerm();

            if (relation.getOperator().equals(Operator.EQ)) {
                firstTables.add(selectorLeft.getName());
                secondTables.add(selectorRight.getName());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("INNER JOIN on: " + selectorRight.getName().getName() + " - "
                            + selectorLeft.getName().getName());
                }
            }

        }

        JavaPairRDD<List<Object>, Cells> rddLeft = leftRdd.mapToPair(new MapKeyForJoin(firstTables));

        JavaPairRDD<List<Object>, Cells> rddRight = rightRdd.mapToPair(new MapKeyForJoin(secondTables));

        if (rddLeft != null && rddRight != null) {
            JavaPairRDD<List<Object>, Tuple2<Cells, Cells>> joinRDD = rddLeft.join(rddRight);

            joinedResult = joinRDD.map(new JoinCells());

        }

        return joinedResult;

    }

    public static Term filterFromRightTermWhereRelation(Relation relation) throws ExecutionException {

        SelectorType type = relation.getRightTerm().getType();
        Term rightField = null;

        switch (type) {
        case STRING:
            rightField = new StringTerm(((StringSelector) relation.getRightTerm()).getValue());
            break;
        case BOOLEAN:
            rightField = new BooleanTerm(((BooleanSelector) relation.getRightTerm()).toString());
            break;
        case INTEGER:
            rightField = new LongTerm(((IntegerSelector) relation.getRightTerm()).toString());
            break;
        case FLOATING_POINT:
            rightField = new DoubleTerm(((FloatingPointSelector) relation.getRightTerm()).toString());
            break;

        default:
            throw new ExecutionException("Unknown Relation Right Term Where found [" + relation.getLeftTerm().getType()
                    + "]");

        }
        return rightField;
    }

    public static Serializable filterFromRightWhereRelation(Relation relation) throws ExecutionException {

        SelectorType type = relation.getRightTerm().getType();
        Serializable rightField = null;

        switch (type) {
        case STRING:
            rightField = String.valueOf(relation.getRightTerm().getStringValue());
            break;
        case BOOLEAN:
            rightField = Boolean.valueOf(((BooleanSelector) relation.getRightTerm()).toString());
            break;
        case INTEGER:
            rightField = Long.valueOf(((IntegerSelector) relation.getRightTerm()).toString());
            break;
        case FLOATING_POINT:
            rightField = Double.valueOf(((FloatingPointSelector) relation.getRightTerm()).toString());
            break;

        default:
            throw new ExecutionException("Unknown Relation Right Term Where found [" + relation.getLeftTerm().getType()
                    + "]");

        }
        return rightField;
    }

    /**
     * @param operator
     * @return String
     */
    public static FilterType retrieveFilterOperator(Operator operator) {

        FilterType filterType = null;
        switch (operator) {
        case EQ:
            filterType = FilterType.EQ;
            break;
        case DISTINCT:
            filterType = FilterType.NEQ;
            break;
        case GET:
            filterType = FilterType.GTE;
            break;
        case GT:
            filterType = FilterType.GT;
            break;
        case LET:
            filterType = FilterType.LTE;
            break;
        case LT:
            filterType = FilterType.LT;
            break;
        case MATCH:
            filterType = FilterType.MATCH;
            break;
        default:
            break;
        }

        return filterType;
    }

    /**
     * Function that returns a grouped {@link JavaRDD} by a list of fields from an initial {@link JavaRDD}.
     * 
     * @param rdd
     *            Initial {@link JavaRDD}.
     * @param ids
     *            List of fields to group by.
     * 
     * @return Grouped {@link JavaRDD}.
     */
    public static JavaRDD<Cells> groupByFields(JavaRDD<Cells> rdd, final List<Selector> selectors) {

        JavaPairRDD<List<Cell>, Cells> rddWithKeys = rdd.keyBy(new Function<Cells, List<Cell>>() {

            private static final long serialVersionUID = 8157822963856298774L;

            @Override
            public List<Cell> call(Cells cells) throws Exception {

                List<Cell> keysList = new ArrayList<>();
                for (Selector selector : selectors) {
                    ColumnSelector columnSelector = (ColumnSelector) selector;
                    Cell cell = cells.getCellByName(columnSelector.getName().getTableName().getQualifiedName(),
                            columnSelector.getName().getName());

                    keysList.add(cell);
                }

                return keysList;
            }
        });

        JavaPairRDD<List<Cell>, Cells> reducedRdd = rddWithKeys.reduceByKey(new Function2<Cells, Cells, Cells>() {

            private static final long serialVersionUID = -2505406515481546086L;

            @Override
            public Cells call(Cells leftCells, Cells rightCells) throws Exception {

                return leftCells;
            }
        });

        return reducedRdd.map(new Function<Tuple2<List<Cell>, Cells>, Cells>() {

            private static final long serialVersionUID = -4921967044782514288L;

            @Override
            public Cells call(Tuple2<List<Cell>, Cells> tuple) throws Exception {
                return tuple._2();
            }
        });
    }

    /**
     * Function that returns a ordered {@link JavaRDD} by a list of fields from an initial {@link JavaRDD}.
     *
     * @param rdd
     *            Initial {@link JavaRDD}.
     * @param ids
     *            List of fields to order by.
     *
     * @return Grouped {@link JavaRDD}.
     */
    public static List<Cells> orderByFields(JavaRDD<Cells> rdd, final List<OrderByClause> orderByClauses) {

        List<Cells> rddOrdered = rdd.takeOrdered((int) rdd.count(), new OrderByComparator(orderByClauses));

        return rddOrdered;

    }


}