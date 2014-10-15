package com.stratio.connector.deep.engine.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import com.stratio.connector.deep.engine.query.functions.DeepEquals;

import com.stratio.connector.deep.engine.query.transformation.FilterColumns;
import com.stratio.connector.deep.engine.query.functions.GreaterEqualThan;
import com.stratio.connector.deep.engine.query.functions.GreaterThan;
import com.stratio.connector.deep.engine.query.functions.LessEqualThan;
import com.stratio.connector.deep.engine.query.functions.LessThan;
import com.stratio.connector.deep.engine.query.functions.NotEquals;
import com.stratio.connector.deep.engine.query.structures.BooleanTerm;
import com.stratio.connector.deep.engine.query.structures.DoubleTerm;
import com.stratio.connector.deep.engine.query.structures.LongTerm;
import com.stratio.connector.deep.engine.query.structures.StringTerm;
import com.stratio.connector.deep.engine.query.structures.Term;
import com.stratio.connector.deep.engine.query.transformation.JoinCells;
import com.stratio.connector.deep.engine.query.transformation.MapKeyForJoin;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.filter.FilterOperator;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.FloatingPointSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.SelectorType;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Created by dgomez on 26/09/14.
 */
public final class QueryFilterUtils {

    /**
     * Class logger.
     */
    private static final Logger logger = Logger.getLogger(QueryFilterUtils.class);

    /**
     * Take a RDD and a Relation and apply suitable filter to the RDD. Execute where clause on Deep.
     * 
     * @param rdd
     *            RDD which filter must be applied.
     * @param relation
     *            {@link com.stratio.meta.common.statements.structures.relationships.Relation} to apply
     * @return A new RDD with the result.
     * @throws UnsupportedException
     */
    static JavaRDD<Cells> doWhere(JavaRDD<Cells> rdd, Relation relation) throws UnsupportedException,
            ExecutionException {

        Operator operator = relation.getOperator();
        JavaRDD<Cells> result = null;
        Serializable field = filterFromLeftTermWhereRelation(relation);
        Term rightTerm =  filterFromRightTermWhereRelation(relation);

        switch (operator) {
        case EQ:
            result = rdd.filter(new DeepEquals(field.toString(), rightTerm));
            break;
        case DISTINCT:
            result = rdd.filter(new NotEquals(field.toString(), rightTerm));
            break;
        case GT:
            result = rdd.filter(new GreaterThan(field.toString(), rightTerm));
            break;
        case GET:
            result = rdd.filter(new GreaterEqualThan(field.toString(), rightTerm));
            break;
        case LT:
            result = rdd.filter(new LessThan(field.toString(), rightTerm));
            break;
        case LET:
            result = rdd.filter(new LessEqualThan(field.toString(), rightTerm));
            break;
        case IN:
            // result = rdd.filter(new In(field, terms));
            throw new UnsupportedException("IN operator unsupported");
        case BETWEEN:
            // result = rdd.filter(new Between(field, terms.get(0), terms.get(1)));
            throw new UnsupportedException("BETWEEN operator unsupported");
        default:
            logger.error("Operator not supported: " + operator);
            result = null;
        }

        return result;
    }

    /**
     * Build JavaRDD<Cells> from list of Cells and select Columns.
     * 
     * @param rdd
     *            Cells RDD
     * @param selectedCols
     *            Set of fields selected in the SelectStatement.
     * @return JavaRDD<Cells>
     */

    static JavaRDD<Cells> filterSelectedColumns(JavaRDD<Cells> rdd, final Set<ColumnName> selectedCols) {

        List<ColumnName> list = new ArrayList<>(selectedCols);
        JavaRDD<Cells> rddResult = rdd.map(new FilterColumns(list));
        rddResult.collect();
        return rddResult;
    }

    static JavaRDD<Cells> doJoin(JavaRDD<Cells> leftRdd, JavaRDD<Cells> rightRdd, List<Relation> joinRelations) {

        JavaRDD<Cells> joinedResult = null;

        List<ColumnName> leftTables = new ArrayList<>();
        List<ColumnName> rightTables = new ArrayList<>();

        for (Relation relation : joinRelations) {

            ColumnSelector selectorRight = (ColumnSelector) relation.getRightTerm();
            ColumnSelector selectorLeft = (ColumnSelector) relation.getLeftTerm();

            if (relation.getOperator().equals(Operator.EQ)) {
                leftTables.add(selectorLeft.getName());
                rightTables.add(selectorRight.getName());
                logger.debug("INNER JOIN on: " + selectorRight.getName().getName() + " - "
                        + selectorLeft.getName().getName());

            }

        }
        leftRdd.collect();
        rightRdd.collect();
        JavaPairRDD<List<Object>, Cells> rddLeft = leftRdd.mapToPair(new MapKeyForJoin(leftTables));
        rddLeft.collect();
        JavaPairRDD<List<Object>, Cells> rddRight = rightRdd.mapToPair(new MapKeyForJoin(rightTables));
        rddRight.collect();
        if (rddLeft != null && rddRight != null) {
            JavaPairRDD<List<Object>, Tuple2<Cells, Cells>> joinRDD = rddLeft.join(rddRight);

            joinedResult = joinRDD.map(new JoinCells());

        }
        joinedResult.collect();
        return joinedResult;

    }

    static Serializable filterFromLeftTermWhereRelation(Relation relation) throws ExecutionException {

        String leftField = null;
        SelectorType type = relation.getLeftTerm().getType();
        switch (type) {
        case STRING:
            leftField = ((StringSelector) relation.getLeftTerm()).getValue();
            break;
        case COLUMN:

            leftField = ((ColumnSelector) relation.getLeftTerm()).getName().getName();
            break;
        default:
            throw new ExecutionException("Unknown Relation Left Selector Where found [" + relation.getLeftTerm()
                    .getType() + "]");

        }
        return leftField;

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

    /**
     * @param operator
     * @return
     */
    public static String retrieveFilterOperator(Operator operator) {

        String operatorName = null;
        switch (operator) {
        case EQ:
            operatorName = FilterOperator.IS;
            break;
        case DISTINCT:
            operatorName = FilterOperator.NE;
            break;
        case GET:
            operatorName = FilterOperator.GTE;
            break;
        case GT:
            operatorName = FilterOperator.GT;
            break;
        case LET:
            operatorName = FilterOperator.LTE;
            break;
        case LT:
            operatorName = FilterOperator.LT;
            break;
        default:
            break;
        }

        return operatorName;
    }
}