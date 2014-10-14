/**
 * 
 */
package com.stratio.connector.deep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Join;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
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
            String data) {

        ColumnSelector leftSelector  = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        StringSelector rightSelector = new StringSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        Filter filter = new Filter(retrieveFilterOperation(operator), relation);

        return filter;
    }

    public static Filter createFilter(String catalogName, String tableName, String columnName, Operator operator,
            Integer data) {

        ColumnSelector leftSelector  = new ColumnSelector(new ColumnName(catalogName, tableName, columnName));
        IntegerSelector rightSelector = new IntegerSelector(data);

        Relation relation = new Relation(leftSelector, operator, rightSelector);

        Filter filter = new Filter(retrieveFilterOperation(operator), relation);

        return filter;
    }


    /**
     * Get the related {@link Operations} to the given {@link Operator}
     * 
     * @param operator
     *            Relation operator
     * @return Operation related to the operator
     */
    public static Operations retrieveFilterOperation(Operator operator) {

        Operations operation = null;
        switch (operator) {
        case EQ:
            operation = Operations.FILTER_FUNCTION_EQ;
            break;
        case GET:
            operation = Operations.FILTER_FUNCTION_GET;
            break;
        case GT:
            operation = Operations.FILTER_FUNCTION_GT;
            break;
        case LET:
            operation = Operations.FILTER_FUNCTION_LET;
            break;
        case LT:
            operation = Operations.FILTER_FUNCTION_LT;
            break;
        case DISTINCT:
            operation = Operations.FILTER_FUNCTION_DISTINCT;
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

    public static ColumnName createColumn(String catalogName, String tableName, String columnName) {

        return new ColumnName(catalogName, tableName, columnName);
    }

    public static Select createSelect(List<ColumnName> columnsList, List<String> aliasNamesList) {

        Map<ColumnName, String> columnsAliases = new LinkedHashMap<>();
        Map<String, ColumnType> columnsTypes = new LinkedHashMap<>();

        Iterator<String> aliasesIt = aliasNamesList.iterator();
        for (ColumnName column : columnsList) {
            columnsAliases.put(column, aliasesIt.next());

            columnsTypes.put(column.getQualifiedName(), ColumnType.BIGINT);
        }

        Select select = new Select(Operations.PROJECT, columnsAliases, columnsTypes);

        return select;
    }
}
