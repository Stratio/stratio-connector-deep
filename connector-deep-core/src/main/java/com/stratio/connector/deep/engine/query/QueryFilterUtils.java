package com.stratio.connector.deep.engine.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.connector.deep.engine.query.functions.DeepEquals;
import com.stratio.connector.deep.engine.query.functions.FilterColumns;
import com.stratio.connector.deep.engine.query.functions.GreaterEqualThan;
import com.stratio.connector.deep.engine.query.functions.GreaterThan;
import com.stratio.connector.deep.engine.query.functions.LessEqualThan;
import com.stratio.connector.deep.engine.query.functions.LessThan;
import com.stratio.connector.deep.engine.query.functions.NotEquals;
import com.stratio.connector.deep.engine.query.structures.SelectTerms;
import com.stratio.connector.deep.engine.query.transformation.JoinCells;
import com.stratio.connector.deep.engine.query.transformation.MapKeyForJoin;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

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
    static JavaRDD<Cells> doWhere(JavaRDD<Cells> rdd, SelectTerms relation) throws UnsupportedException {

        String operator = relation.getOperation();
        JavaRDD<Cells> result = null;
        String field = relation.getField();
        Serializable rightTerm = relation.getValue();

        logger.info("Rdd doWhere input size: " + rdd.count());
        switch (operator.toString().toLowerCase()) {
        case "=":
            result = rdd.filter(new DeepEquals(field, rightTerm));
            break;
        case "<>":
            result = rdd.filter(new NotEquals(field, rightTerm));
            break;
        case ">":
            result = rdd.filter(new GreaterThan(field, rightTerm));
            break;
        case ">=":
            result = rdd.filter(new GreaterEqualThan(field, rightTerm));
            break;
        case "<":
            result = rdd.filter(new LessThan(field, rightTerm));
            break;
        case "<=":
            result = rdd.filter(new LessEqualThan(field, rightTerm));
            break;
        case "in":
            // result = rdd.filter(new In(field, terms));
            throw new UnsupportedException("IN operator unsupported");
        case "between":
            // result = rdd.filter(new Between(field, terms.get(0), terms.get(1)));
            throw new UnsupportedException("BETWEEN operator unsupported");
        default:
            logger.error("Operator not supported: " + operator);
            result = null;
        }


        logger.info("Rdd doWhere output size: " + result.count());

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
        JavaRDD<Cells> rddResult = rdd.map(new FilterColumns(list)) ;
        return rddResult;
    }

    static JavaRDD<Cells> doJoin(JavaRDD<Cells> leftRdd, JavaRDD<Cells> rightRdd, List<Relation> joinRelations) {

        JavaRDD<Cells> joinedResult = null ;


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

        JavaPairRDD<List<Object>, Cells> rddLeft = leftRdd.mapToPair(new MapKeyForJoin(leftTables));
        List<Tuple2<List<Object>, Cells>> t = rddLeft.collect();
        JavaPairRDD<List<Object>, Cells> rddRight = rightRdd.mapToPair(new MapKeyForJoin(rightTables));
        List<Tuple2<List<Object>, Cells>> t2 = rddRight.collect();
        if(rddLeft!=null && rddRight!=null){
            JavaPairRDD<List<Object>, Tuple2<Cells, Cells>> joinRDD = rddLeft.join(rddRight);

            joinedResult = joinRDD.map(new JoinCells());


        }

        return joinedResult;

    }
}