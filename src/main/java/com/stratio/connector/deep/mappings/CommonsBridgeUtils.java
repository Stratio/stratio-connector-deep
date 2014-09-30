package com.stratio.connector.deep.mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.stratio.connector.deep.mappings.functions.DeepEquals;
import com.stratio.connector.deep.mappings.functions.GreaterEqualThan;
import com.stratio.connector.deep.mappings.functions.GreaterThan;
import com.stratio.connector.deep.mappings.functions.JoinCells;
import com.stratio.connector.deep.mappings.functions.LessEqualThan;
import com.stratio.connector.deep.mappings.functions.LessThan;
import com.stratio.connector.deep.mappings.functions.MapKeyForJoin;
import com.stratio.connector.deep.mappings.functions.NotEquals;
import com.stratio.connector.deep.mappings.structures.GroupBy;
import com.stratio.connector.deep.mappings.transformation.AverageAggregatorMapping;
import com.stratio.connector.deep.mappings.transformation.GroupByAggregation;
import com.stratio.connector.deep.mappings.transformation.GroupByMapping;
import com.stratio.connector.deep.mappings.transformation.KeyRemover;
import com.stratio.connector.deep.mappings.trasfer.ColumnInfo;
import com.stratio.connector.deep.mappings.utils.DeepUtils;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta.common.statements.structures.selectors.GroupByFunction;
import com.stratio.meta.core.structures.SelectionList;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

import scala.Tuple2;

/**
 * Created by dgomez on 26/09/14.
 */
public class CommonsBridgeUtils {


    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(CommonsBridgeUtils.class);



        /**
         * Take a RDD and a Relation and apply suitable filter to the RDD. Execute where clause on Deep.
         *
         * @param rdd      RDD which filter must be applied.
         * @param relation {@link com.stratio.meta.common.statements.structures.relationships.Relation} to apply
         * @return A new RDD with the result.
         */
        public JavaRDD<Cells> doWhere(JavaRDD<Cells> rdd, Relation relation) {

            Operator operator = relation.getOperator();
            JavaRDD<Cells> result = null;
            Selector leftTerm = relation.getLeftTerm();
            Selector rightTerm = relation.getRightTerm();

            ColumnSelector columnSelector = (ColumnSelector)leftTerm;
            String field = columnSelector.getName().getName();

            LOG.info("Rdd input size: " + rdd.count());
            switch (operator.toString().toLowerCase()) {
            case "=":
                result = rdd.filter(new DeepEquals(field,rightTerm));
                break;
            case "<>":
                result = rdd.filter(new NotEquals(field, rightTerm));
                break;
            case ">":
                result = rdd.filter(new GreaterThan(field, rightTerm));
                break;
            case ">=":
                result = rdd.filter(new GreaterEqualThan(field,rightTerm));
                break;
            case "<":
                result = rdd.filter(new LessThan(field, rightTerm));
                break;
            case "<=":
                result = rdd.filter(new LessEqualThan(field, rightTerm));
                break;
            case "in":
                //result = rdd.filter(new In(field, terms));
                //TODO throw new UnsupportedException("");
                break;
            case "between":
                //result = rdd.filter(new Between(field, terms.get(0), terms.get(1)));
                //TODO throw new UnsupportedException("");
                break;
            default:
                LOG.error("Operator not supported: " + operator);
                result = null;
            }
            return result;
        }

        /**
         * Take a RDD and the group by information, and apply the requested grouping. If there is any
         * aggregation function, apply it to the desired column.
         *
         * @param rdd RDD which filter must be applied.
         * @param groupByClause {@link com.stratio.meta.core.structures.GroupBy} to retrieve the grouping
         *        columns.
         * @param selectionClause {@link com.stratio.meta.core.structures.SelectionClause} containing the
         *        aggregation functions.
         * @return A new RDD with the result.
         */
        public JavaRDD<Cells> doGroupBy(JavaRDD<Cells> rdd, List<GroupBy> groupByClause, SelectionList selectionClause) {

            final List<ColumnInfo> aggregationCols;
            if (selectionClause != null) {

                aggregationCols = DeepUtils.retrieveSelectorAggegationFunctions(selectionClause.getSelection());
            } else {
                aggregationCols = null;
            }

            // Mapping the rdd to execute the group by clause
            JavaPairRDD<Cells, Cells> groupedRdd = rdd.mapToPair(new GroupByMapping(aggregationCols, groupByClause));

            if (selectionClause != null) {
                groupedRdd = applyGroupByAggregations(groupedRdd, aggregationCols);
            }

            JavaRDD<Cells> map = groupedRdd.map(new KeyRemover());

            return map;
        }

        private JavaPairRDD<Cells, Cells> applyGroupByAggregations(JavaPairRDD<Cells, Cells> groupedRdd,
                List<ColumnInfo> aggregationCols) {

            JavaPairRDD<Cells, Cells> aggregatedRdd =
                    groupedRdd.reduceByKey(new GroupByAggregation(aggregationCols));

            // Looking for the average aggregator to complete it
            for (ColumnInfo aggregation : aggregationCols) {

                if (GroupByFunction.AVG == aggregation.getAggregationFunction()) {
                    aggregatedRdd = aggregatedRdd.mapValues(new AverageAggregatorMapping(aggregation));
                }
            }
            return aggregatedRdd;
        }

    /**
     * Build JavaRDD<Cells> from list of Cells and select Columns.
     *
     * @param rdd          list of JavaRDD Cells
     * @param selectedCols List of fields selected in the SelectStatement.
     * @return JavaRDD<Cells>
     */
    public static JavaRDD<Cells> returnSelect(JavaRDD<Cells> rdd, Select selectedCols) {

        final Set<String> maps = selectedCols.getColumnMap().keySet();

        JavaRDD<Cells> cellsInRDD = rdd.map(new Function<Cells, Cells>() {
            @Override
            public Cells call(Cells cells) throws Exception {
                Cells cellsout = new Cells();
                for (Cell deepCell : cells.getCells()) {
                    if (maps.contains(deepCell.getCellName())) {
                        cellsout.add(deepCell);
                    }
                }

                return cellsout;
            }
        });

        return cellsInRDD;
    }



}