package com.stratio.connector.deep.engine.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.connector.deep.engine.query.functions.DeepEquals;
import com.stratio.connector.deep.engine.query.functions.GreaterEqualThan;
import com.stratio.connector.deep.engine.query.functions.GreaterThan;
import com.stratio.connector.deep.engine.query.functions.JoinCells;
import com.stratio.connector.deep.engine.query.functions.LessEqualThan;
import com.stratio.connector.deep.engine.query.functions.LessThan;
import com.stratio.connector.deep.engine.query.functions.MapKeyForJoin;
import com.stratio.connector.deep.engine.query.functions.NotEquals;
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
    static JavaRDD<Cells> doWhere(JavaRDD<Cells> rdd, Relation relation) throws UnsupportedException {

        Operator operator = relation.getOperator();
        JavaRDD<Cells> result = null;
        Selector leftTerm = relation.getLeftTerm();
        Selector rightTerm = relation.getRightTerm();

        ColumnSelector columnSelector = (ColumnSelector) leftTerm;
        String field = columnSelector.getName().getName();

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
     * Take a RDD and the group by information, and apply the requested grouping. If there is any aggregation function,
     * apply it to the desired column.
     * 
     * @param rdd
     *            RDD which filter must be applied.
     * @param groupByClause
     *            {@link com.stratio.meta.core.structures.GroupBy} to retrieve the grouping columns.
     * @param selectionClause
     *            {@link com.stratio.meta.core.structures.SelectionClause} containing the aggregation functions.
     * @return A new RDD with the result.
     */
    // public static JavaRDD<Cells> doGroupBy(JavaRDD<Cells> rdd, List<GroupBy> groupByClause, SelectionList
    // selectionClause) {
    //
    // final List<ColumnInfo> aggregationCols;
    // if (selectionClause != null) {
    //
    // aggregationCols = DeepUtils.retrieveSelectorAggegationFunctions(selectionClause.getSelection());
    // } else {
    // aggregationCols = null;
    // }
    //
    // // Mapping the rdd to execute the group by clause
    // JavaPairRDD<Cells, Cells> groupedRdd = rdd.mapToPair(new GroupByMapping(aggregationCols, groupByClause));
    //
    // if (selectionClause != null) {
    // groupedRdd = applyGroupByAggregations(groupedRdd, aggregationCols);
    // }
    //
    // JavaRDD<Cells> map = groupedRdd.map(new KeyRemover());
    //
    // return map;
    // }
    //
    // private JavaPairRDD<Cells, Cells> applyGroupByAggregations(JavaPairRDD<Cells, Cells> groupedRdd,
    // List<ColumnInfo> aggregationCols) {
    //
    // JavaPairRDD<Cells, Cells> aggregatedRdd =
    // groupedRdd.reduceByKey(new GroupByAggregation(aggregationCols));
    //
    // // Looking for the average aggregator to complete it
    // for (ColumnInfo aggregation : aggregationCols) {
    //
    // if (GroupByFunction.AVG == aggregation.getAggregationFunction()) {
    // aggregatedRdd = aggregatedRdd.mapValues(new AverageAggregatorMapping(aggregation));
    // }
    // }
    // return aggregatedRdd;
    // }

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

        return rdd.map(new Function<Cells, Cells>() {

            private static final long serialVersionUID = -5704730871386839898L;

            @Override
            public Cells call(Cells cells) throws Exception {
                Cells cellsOut = new Cells();

                for (ColumnName columnName : selectedCols) {
                    Cell cell = cells.getCellByName(columnName.getTableName().getQualifiedName(), columnName.getName());
                    cellsOut.add(cell);
                }

                return cellsOut;
            }
        });
    }

    static JavaRDD<Cells> doJoin(JavaRDD<Cells> leftRdd, JavaRDD<Cells> rightRdd, List<Relation> joinRelations) {

        List<ColumnName> leftTables = new ArrayList<>();
        List<ColumnName> rightTables = new ArrayList<>();

        for (Relation relation : joinRelations) {

            ColumnSelector selectorRight = (ColumnSelector) relation.getRightTerm();
            ColumnSelector selectorLeft  = (ColumnSelector) relation.getLeftTerm();

            if (relation.getOperator().equals(Operator.EQ)) {
                leftTables.add(selectorLeft.getName());
                rightTables.add(selectorRight.getName());
                System.out.println("INNER JOIN on: " + selectorRight.getName().getName() + " - " + selectorLeft.getName().getName());
            }

        }

        JavaPairRDD<Cells, Cells> rddLeft  = leftRdd.mapToPair(new MapKeyForJoin<Cells>(leftTables));
        logger.debug("------------------Left count is :"+rddLeft.count());
        List<Tuple2<Cells, Cells>> lista = rddLeft.collect();
        logger.debug("**************************************************************");
        logger.debug("---------------------imprimo el left1 " + rddLeft.first()._1());
        logger.debug("---------------------imprimo el left2 " + rddLeft.first()._2());
        logger.debug("**************************************************************");
        JavaPairRDD<Cells, Cells> rddRight = rightRdd.mapToPair(new MapKeyForJoin<Cells>(rightTables));
        logger.debug("------------------Right count is :"+rddRight.count());
        List<Tuple2<Cells, Cells>> lista2 = rddRight.collect();
        logger.debug("**************************************************************");
        logger.debug("------------imprimo el right1 " + rddRight.first()._1());
        logger.debug("------------imprimo el right2 " + rddRight.first()._2());
        logger.debug("**************************************************************");
        JavaPairRDD<Cells, Tuple2<Cells, Cells>> joinRDD = rddLeft.join(rddRight);
        logger.debug("-----------------Result count is :" + joinRDD.count()+"---------------------");

        JavaRDD<Cells> joinedResult = joinRDD.map(new JoinCells<Cells>());

        return joinedResult;



    }

}