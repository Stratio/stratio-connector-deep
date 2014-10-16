/**
 * 
 */
package com.stratio.connector.deep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.PartialResults;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.statements.structures.selectors.BooleanSelector;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;

import com.stratio.meta2.common.statements.structures.selectors.FloatingPointSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;

import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * Builder class to create valid logical steps and workflows
 */
public class LogicalWorkflowBuilder {

    public static Project createProject(String clusterName, String catalogName, String tableName,
            List<String> columnList) {

        List<ColumnName> columns = new ArrayList<>();
        for (String column : columnList) {
            columns.add(new ColumnName(catalogName, tableName, column));
        }

        TableName table = new TableName(catalogName, tableName);
        Project project = new Project(Operations.PROJECT, table, new ClusterName(clusterName), columns);

        return project;
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
            Serializable data, boolean indexed) {


        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        Selector rightSelector = null;
        if (data instanceof String) {
            rightSelector = new StringSelector((String) data);
        } else if (data instanceof Integer) {
            rightSelector = new IntegerSelector((Integer) data);
        }


        Relation relation = new Relation(leftSelector, operator, rightSelector);

        Filter filter = new Filter(retrieveFilterOperation(operator, indexed), relation);

        return filter;
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
            Double data, boolean indexed) {

        ColumnSelector leftSelector  = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        FloatingPointSelector rightSelector = new FloatingPointSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        Filter filter = new Filter(retrieveFilterOperation(operator,indexed), relation);

        return filter;
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
            Integer data, boolean indexed) {

        ColumnSelector leftSelector  = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        IntegerSelector rightSelector = new IntegerSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        Filter filter = new Filter(retrieveFilterOperation(operator,indexed), relation);

        return filter;
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
            Boolean data, boolean indexed) {

        ColumnSelector leftSelector  = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        BooleanSelector rightSelector = new BooleanSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        Filter filter = new Filter(retrieveFilterOperation(operator,indexed), relation);

        return filter;
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
            Long data, boolean indexed) {

        ColumnSelector leftSelector  = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        IntegerSelector rightSelector = new IntegerSelector(data.toString());

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        Filter filter = new Filter(retrieveFilterOperation(operator,indexed), relation);

        return filter;
    }
    /**
     * Get the related {@link Operations} to the given {@link Operator}
     * 
     * @param operator
     *            Relation operator
     * @return Operation related to the operator
     */
    public static Operations retrieveFilterOperation(Operator operator, boolean indexed) {

        Operations operation = null;
        switch (operator) {
        case EQ:
            operation = indexed ? Operations.FILTER_INDEXED_EQ : Operations.FILTER_NON_INDEXED_EQ;
            break;
        case GET:
            operation = indexed ? Operations.FILTER_INDEXED_GET : Operations.FILTER_NON_INDEXED_GET;
            break;
        case GT:
            operation = indexed ? Operations.FILTER_INDEXED_GT : Operations.FILTER_NON_INDEXED_GT;
            break;
        case LET:
            operation = indexed ? Operations.FILTER_INDEXED_LET : Operations.FILTER_NON_INDEXED_LET;
            break;
        case LT:
            operation = indexed ? Operations.FILTER_INDEXED_LT : Operations.FILTER_NON_INDEXED_LT;
            break;
        case DISTINCT:
            operation = indexed ? Operations.FILTER_INDEXED_DISTINCT : Operations.FILTER_NON_INDEXED_DISTINCT;
            break;
        case MATCH:
            operation = Operations.FILTER_FULLTEXT;
            break;
        default:
            break;
        }

        return operation;
    }

    public static Join createJoin(String joinId, ColumnName leftSource, ColumnName rightSource) {

        ColumnSelector leftSelector = new ColumnSelector(leftSource);
        ColumnSelector rightSelector = new ColumnSelector(rightSource);

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        Join join = new Join(Operations.SELECT_INNER_JOIN, joinId);
        join.addJoinRelation(relation);
        join.addSourceIdentifier(leftSource.getTableName().getQualifiedName());
        join.addSourceIdentifier(rightSource.getTableName().getQualifiedName());

        return join;
    }

    public static Join createJoinPartialResults(String joinId, ColumnName leftSource, ColumnName rightSource,
            List<ColumnMetadata> columnMetadata, List<Row> rows) {

        ColumnSelector leftSelector = new ColumnSelector(leftSource);
        ColumnSelector rightSelector = new ColumnSelector(rightSource);

        Relation relation = new Relation(rightSelector, Operator.EQ, leftSelector);

        Join join = new Join(Operations.SELECT_INNER_JOIN_PARTIALS_RESULTS, joinId);
        join.addJoinRelation(relation);
        join.addSourceIdentifier(leftSource.getTableName().getQualifiedName());
        join.addSourceIdentifier(rightSource.getTableName().getQualifiedName());

        PartialResults partialResults = new PartialResults(Operations.PARTIAL_RESULTS);
        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(columnMetadata);
        resultSet.setRows(rows);
        partialResults.setResults(resultSet);
        join.addPreviousSteps(partialResults);

        return join;
    }

    public static ColumnName createColumn(String catalogName, String tableName, String columnName) {

        return new ColumnName(catalogName, tableName, columnName);
    }

    public static Select createSelect(List<ColumnName> columnsList, List<String> aliasNamesList) {

        Map<ColumnName, String> columnsAliases = new LinkedHashMap<>();
        Map<String, ColumnType> columnsTypes = new LinkedHashMap<>();

        Iterator<String> aliasesIt = aliasNamesList.iterator();
        for (ColumnName column : columnsList) {
            columnsAliases.put(column, aliasesIt.next());

            columnsTypes.put(column.getQualifiedName(), ColumnType.TEXT);
        }

        Select select = new Select(Operations.PROJECT, columnsAliases, columnsTypes);

        return select;
    }
}
