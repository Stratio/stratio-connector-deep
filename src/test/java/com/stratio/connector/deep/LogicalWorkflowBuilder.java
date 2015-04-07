/**
 *
 */
package com.stratio.connector.deep;

import java.io.Serializable;
import java.util.*;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.GroupBy;
import com.stratio.crossdata.common.logicalplan.Join;
import com.stratio.crossdata.common.logicalplan.OrderBy;
import com.stratio.crossdata.common.logicalplan.PartialResults;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.BooleanSelector;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FloatingPointSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

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
        HashSet<Operations> operations = new HashSet<>();
        operations.add(Operations.PROJECT);

        return new Project(operations, table, new ClusterName(clusterName), columns);
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
                                      Serializable data, boolean indexed) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        Selector rightSelector = null;
        if (data instanceof String) {
            rightSelector = new StringSelector((String) data);
        } else if (data instanceof Integer) {
            rightSelector = new IntegerSelector((Integer) data);
        } else if (data instanceof Long) {
            rightSelector = new IntegerSelector(data.toString());
        } else if (data instanceof Double) {
            rightSelector = new FloatingPointSelector((Double) data);
        } else if (data instanceof Float) {
            rightSelector = new FloatingPointSelector((Float) data);
        }

        return new Filter(retrieveFilterOperation(operator, indexed), new Relation(leftSelector, operator, rightSelector));
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
                                      Double data, boolean indexed) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        FloatingPointSelector rightSelector = new FloatingPointSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        return new Filter(retrieveFilterOperation(operator, indexed), relation);
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
                                      Integer data, boolean indexed) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        IntegerSelector rightSelector = new IntegerSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        return new Filter(retrieveFilterOperation(operator, indexed), relation);
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
                                      Float data, boolean indexed) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        FloatingPointSelector rightSelector = new FloatingPointSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        return new Filter(retrieveFilterOperation(operator, indexed), relation);
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
                                      Boolean data, boolean indexed) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        BooleanSelector rightSelector = new BooleanSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        return new Filter(retrieveFilterOperation(operator, indexed), relation);
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
                                      Long data, boolean indexed) {

        ColumnSelector leftSelector = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        IntegerSelector rightSelector = new IntegerSelector(data.toString());

        return new Filter(retrieveFilterOperation(operator, indexed), new Relation(leftSelector, operator, rightSelector));
    }

    /**
     * Get the related {@link Operations} to the given {@link Operator}
     *
     * @param operator
     *            Relation operator
     * @return Operation related to the operator
     */
    public static Set<Operations> retrieveFilterOperation(Operator operator, boolean indexed) {

        Set<Operations> operations = new HashSet<>();
        switch (operator) {
            case EQ:
                if (indexed)
                    operations.add(Operations.FILTER_INDEXED_EQ);
                else
                    operations.add(Operations.FILTER_NON_INDEXED_EQ);
            break;
            case GET:
                if (indexed)
                    operations.add(Operations.FILTER_INDEXED_GET);
                else
                    operations.add(Operations.FILTER_NON_INDEXED_GET);
                break;
            case GT:
                if (indexed)
                    operations.add(Operations.FILTER_INDEXED_GT);
                else
                    operations.add(Operations.FILTER_NON_INDEXED_GT);
            break;
            case LET:
                if (indexed)
                    operations.add(Operations.FILTER_INDEXED_LET);
                else
                    operations.add(Operations.FILTER_NON_INDEXED_LET);
            break;
            case LT:
                if (indexed)
                    operations.add(Operations.FILTER_INDEXED_LT);
                else
                    operations.add(Operations.FILTER_NON_INDEXED_LT);
            break;
            case DISTINCT:
                if (indexed)
                    operations.add(Operations.FILTER_INDEXED_DISTINCT);
                else
                    operations.add(Operations.FILTER_NON_INDEXED_DISTINCT);
            break;
            case MATCH:
                operations.add(Operations.FILTER_INDEXED_MATCH);
            break;
            default:
                break;
        }

        return operations;
    }

    public static Join createJoin(String joinId, ColumnName leftSource, ColumnName rightSource) {

        ColumnSelector leftSelector = new ColumnSelector(leftSource);
        ColumnSelector rightSelector = new ColumnSelector(rightSource);

        Relation relation = new Relation(leftSelector, Operator.EQ, rightSelector);

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.SELECT_INNER_JOIN);

        Join join = new Join(operations, joinId);
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

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.SELECT_INNER_JOIN_PARTIALS_RESULTS);

        Join join = new Join(operations, joinId);
        join.addJoinRelation(relation);
        join.addSourceIdentifier(leftSource.getTableName().getQualifiedName());
        join.addSourceIdentifier(rightSource.getTableName().getQualifiedName());

        Set<Operations> operations2 = new HashSet<>();
        operations2.add(Operations.PARTIAL_RESULTS);

        PartialResults partialResults = new PartialResults(operations2);
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

        Map<Selector, String> columnsAliases = new LinkedHashMap<>();
        Map<String, ColumnType> columnsTypes = new LinkedHashMap<>();
        Map<Selector, ColumnType> typeMapFromColumnName = new LinkedHashMap<>();

        Iterator<String> aliasesIt = aliasNamesList.iterator();
        for (ColumnName column : columnsList) {
            ColumnSelector columnSelector = new ColumnSelector(column);
            columnsAliases.put(columnSelector, aliasesIt.next());

            columnsTypes.put(column.getQualifiedName(), new ColumnType(DataType.TEXT));
            typeMapFromColumnName.put(columnSelector, new ColumnType(DataType.TEXT));
        }

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.PROJECT);

        return new Select(operations, columnsAliases, columnsTypes, typeMapFromColumnName);
    }

    public static GroupBy createGroupBy(List<ColumnName> columnsList) {

        List<Selector> selectorsList = new LinkedList<>();
        for (ColumnName column : columnsList) {
            ColumnSelector selector = new ColumnSelector(column);
            selectorsList.add(selector);
        }

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.SELECT_GROUP_BY);

        return new GroupBy(operations, selectorsList);
    }

    public static  OrderBy createOrderBy(String keyspace, String table, LinkedHashMap<String,
            OrderDirection> orderByFields) {

        Set<Operations> operations = new HashSet<>();
        operations.add(Operations.SELECT_ORDER_BY);

        OrderBy  orderBy = new OrderBy(operations, new LinkedList<OrderByClause>());

        List<OrderByClause> previousList = orderBy.getIds();

        for(String field :orderByFields.keySet()){
            OrderDirection direction = orderByFields.get(field);
            previousList.add(new OrderByClause(direction, new ColumnSelector(new ColumnName(keyspace, table,
                    field))));
        }

        orderBy.setIds(previousList);

        return orderBy;
    }

}
