package com.stratio.connector.deep;

import java.io.Serializable;

import com.stratio.crossdata.common.data.Row;

public class RowComparator implements java.util.Comparator<Row>,Serializable {



    private static final String AUTHOR_CONSTANT = "artist";

    private static final String YEAR_CONSTANT = "year";

    public RowComparator() {
    }

    @Override
    public int compare(Row row1, Row row2) {

        int result = 0;

        int row1Year = Integer.valueOf(row1.getCell(YEAR_CONSTANT).getValue().toString());
        int row2Year = Integer.valueOf(row2.getCell(YEAR_CONSTANT).getValue().toString());
        int row1Author = Integer.valueOf(row1.getCell(AUTHOR_CONSTANT).getValue().toString());
        int row2Author = Integer.valueOf(row2.getCell(AUTHOR_CONSTANT).getValue().toString());


        return 0;
    }
}
