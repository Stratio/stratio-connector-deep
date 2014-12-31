package com.stratio.connector.deep.engine.query.functions;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;

public class OrderByComparator implements Comparator<Cells>,Serializable {

    private static final long serialVersionUID = 432384912608139416L;

    /**
     * Term to compare.
     */
    private List<OrderByClause> orderByClauses;

    public OrderByComparator(List<OrderByClause> orderByClauses) {
        this.orderByClauses = orderByClauses;
    }


    @Override
    public int compare(Cells o1, Cells o2) {

        int result = 0;

        for (OrderByClause orderByClause : orderByClauses) {

            ColumnSelector columnSelector = (ColumnSelector) orderByClause.getSelector();

            Cell cell1 = o1.getCellByName(columnSelector.getName().getTableName().getQualifiedName(),
                    columnSelector.getName().getName());

            Cell cell2 = o2.getCellByName(columnSelector.getName().getTableName().getQualifiedName(),
                    columnSelector.getName().getName());

            OrderDirection order = orderByClause.getDirection();

            if(order == OrderDirection.ASC){
                result = ((Comparable) cell1.getCellValue()).compareTo(cell2.getValue());
            }else{
                result = ((Comparable) cell2.getCellValue()).compareTo(cell1.getValue());
            }
            if(result!=0){
                break;
            }

        }

        return result;

    }
}
