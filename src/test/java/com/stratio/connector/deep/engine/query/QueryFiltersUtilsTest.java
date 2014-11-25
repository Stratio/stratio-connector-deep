package com.stratio.connector.deep.engine.query;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import scala.Tuple2;

import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.deep.connection.DeepConnection;
import com.stratio.connector.deep.connection.DeepConnectionHandler;
import com.stratio.connector.deep.engine.query.functions.DeepEquals;
import com.stratio.connector.deep.engine.query.transformation.JoinCells;
import com.stratio.connector.deep.engine.query.transformation.MapKeyForJoin;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;

/**
 * Created by dgomez on 30/09/14.
 */
@RunWith(PowerMockRunner.class)
public class QueryFiltersUtilsTest implements Serializable {

    private static final Logger logger = Logger.getLogger(QueryFiltersUtilsTest.class);

    private static final String CASSANDRA_CELL_CLASS = "com.stratio.deep.cassandra.extractor.CassandraCellExtractor";
    private static final String MONGO_CELL_CLASS = "com.stratio.deep.mongodb.extractor.MongoCellExtractor";
    private static final String ES_CELL_CLASS = "com.stratio.deep.extractor.ESCellExtractor";

    private static final String CATALOG_CONSTANT = "test";

    private static final TableName TABLE1_CONSTANT = new TableName(CATALOG_CONSTANT, "mytable");

    private static final TableName TABLE2_CONSTANT = new TableName(CATALOG_CONSTANT, "mytable1");

    private static final String COLUMN1_CONSTANT = "thekey";

    private static final String COLUMN2_CONSTANT = "thekey";

    private static final ClusterName CLUSTERNAME_CONSTANT = new ClusterName("clusterName");

    private static final String DATA_CONSTANT = "001";
    @Mock
    private DeepSparkContext deepContext;

    @Mock
    private DeepConnectionHandler deepConnectionHandler;

    @Mock
    private DeepConnection deepConnection;

    @Mock
    private ExtractorConfig<Cells> extractorConfig;

    @Mock(name = "leftRdd")
    private JavaRDD<Cells> leftRdd;

    @Mock(name = "rightRdd")
    private JavaRDD<Cells> rightRdd;

    @Mock(name = "thirdRdd")
    private JavaRDD<Cells> thirdRdd;

    @Mock(name = "rddLeft")
    JavaPairRDD<List<Object>, Cells> rddLeft;

    @Mock(name = "rddRight")
    JavaPairRDD<List<Object>, Cells> rddRight;

    @Mock(name = "joinRDD")
    JavaPairRDD<List<Object>, Tuple2<Cells, Cells>> joinRDD;

    @Before
    public void before() throws Exception, HandlerConnectionException {
        String job = "java:creatingCellRDD";

        when(deepConnectionHandler.getConnection(CLUSTERNAME_CONSTANT.getName())).thenReturn(deepConnection);
        when(deepConnection.getExtractorConfig()).thenReturn(extractorConfig);
        when(deepContext.createJavaRDD(any(ExtractorConfig.class))).thenReturn(leftRdd, rightRdd);

        when(leftRdd.mapToPair(any(MapKeyForJoin.class))).thenReturn(rddLeft, rddRight);

    }

    @After
    public void after() throws Exception, HandlerConnectionException {

        // deepContext.stop();
        // ExtractorServer.close();
    }

    @Test
    public void doWhereTest() throws UnsupportedException, ExecutionException, Exception {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT));

        IntegerSelector rightSelector = new IntegerSelector(DATA_CONSTANT);

        // SelectTerms selectTerms = new SelectTerms( leftSelector.getName().getName(), Operator.EQ.toString(),
        // rightSelector);
        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        JavaRDD<Cells> rdd = QueryFilterUtils.doWhere(leftRdd, relation);
        if (logger.isDebugEnabled()) {
            logger.debug("-------------------resultado Encontrados--------------" + rdd.count());
            logger.debug("---------------il----resultado de filterSelectedColumns--------------" + rdd.first()
                    .toString());
        }

        // verifyPrivate(queryFilterUtils).invoke("filterFromLeftTermWhereRelation");
        verify(leftRdd, times(1)).filter(any(DeepEquals.class));

    }

    @Test
    public void filterSelectedColumns() {
        Map<ColumnName, String> columnsAliases = new HashMap<>();

        columnsAliases.put(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT), "nameAlias");

        JavaRDD<Cells> rdd = QueryFilterUtils.filterSelectedColumns(leftRdd, columnsAliases.keySet());
        if (logger.isDebugEnabled()) {
            logger.debug("-------------------resultado de filterSelectedColumns--------------" + rdd.first().toString());
        }
        verify(leftRdd, times(1)).map(any(Function.class));

    }

    @Test
    public void doJoinTest() {

        List<Relation> relations = new ArrayList<Relation>();
        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE1_CONSTANT.getName(),
                COLUMN1_CONSTANT));

        ColumnSelector rightSelector = new ColumnSelector(new ColumnName(CATALOG_CONSTANT, TABLE2_CONSTANT.getName(),
                COLUMN2_CONSTANT));

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);
        relations.add(relation);

        JavaRDD<Cells> outputrdd = QueryFilterUtils.doJoin(leftRdd, rightRdd, relations);
        if (logger.isDebugEnabled()) {

            logger.debug("El resultado es :" + outputrdd.count());
            logger.debug("resultado " + outputrdd.first().toString());

            int i = 0;
            for (Cells cell : outputrdd.collect()) {

                logger.debug("-----------------resultado " + (i++) + "  " + cell.getCellValues());

            }
        }
        verify(leftRdd, times(1)).mapToPair(any(MapKeyForJoin.class));
        verify(rightRdd, times(1)).mapToPair(any(MapKeyForJoin.class));

        verify(rddLeft, times(0)).join(rddRight);
        verify(joinRDD, times(0)).map(any(JoinCells.class));

    }

}
