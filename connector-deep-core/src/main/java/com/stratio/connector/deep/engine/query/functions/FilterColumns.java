package com.stratio.connector.deep.engine.query.functions;

import java.util.List;

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.meta2.common.data.ColumnName;

public class FilterColumns implements Function<Cells, Cells> {

    /*
     * 
     * Filter Columns
     */
    private final List<ColumnName> columns;

    /**
     *
     *
     *
     */
    public FilterColumns(List<ColumnName> columns) {
        this.columns = columns;
    }

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -6143471789450703044L;

    @Override
    public Cells call(Cells cells) throws Exception {

        Cells cellsOut = new Cells();
        for (ColumnName columnName : columns) {
            Cell cell = cells.getCellByName(columnName.getTableName().getQualifiedName(), columnName.getName());
            cellsOut.add(columnName.getTableName().getQualifiedName(), cell);
        }

        return cellsOut;
    }
}
