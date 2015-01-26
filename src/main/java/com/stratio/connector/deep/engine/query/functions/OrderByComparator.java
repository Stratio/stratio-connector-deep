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

package com.stratio.connector.deep.engine.query.functions;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;

/**
 * 
 * Class that defines OrderBy, Spark function that sorts the records in ascending order by default.
 *
 */
public class OrderByComparator implements Comparator<Cells>,Serializable {

    private static final long serialVersionUID = 432384912608139416L;

    /**
     * Order clauses for the OrderBy function.
     */
    private List<OrderByClause> orderByClauses;

    /**
     * Basic constructor for the OrderBy function class.
     * @param orderByClauses
     * 						Order clauses for the OrderBy function
     */
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
