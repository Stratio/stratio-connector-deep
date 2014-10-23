package com.stratio.connector.deep.engine.query;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.PartialResults;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.deep.commons.entity.Cells;

@RunWith(PowerMockRunner.class)
@PrepareForTest(QueryPartialResultsUtils.class)
public class QueryPartialResultsUtilsTest {

    static final String TABLE = "table";
    static final String CATALOG = "catalog";
    static final String OTHER_CATALOG = "otherCatalog";
    static final String ROW1 = "id";
    static final String ROW2 = "name";
    static final Integer CELL1_VALUE1 = 1;
    static final String CELL1_VALUE2 = "deep";
    static final Integer CELL2_VALUE1 = 2;
    static final String CELL2_VALUE2 = "connectors";

    @Test
    public void getColumnNameFromQualifiedColumnNameTest() throws ExecutionException {
        try {
            QueryPartialResultsUtils.getColumnNameFromQualifiedColumnName(null);
            Assert.assertTrue("An exception should be thrown", false);
        } catch (ExecutionException e) {
        }
        try {
            QueryPartialResultsUtils.getColumnNameFromQualifiedColumnName("");
            Assert.assertTrue("An exception should be thrown", false);
        } catch (ExecutionException e) {
        }
        try {
            QueryPartialResultsUtils.getColumnNameFromQualifiedColumnName("table.column");
            Assert.assertTrue("An exception should be thrown", false);
        } catch (ExecutionException e) {
        }

        String column = QueryPartialResultsUtils.getColumnNameFromQualifiedColumnName("catalog.table.column");
        Assert.assertEquals("column", column);

    }

    @Test
    public void getOrderedRelationsTest() throws ExecutionException {
        ColumnSelector colSelectorLeft = new ColumnSelector(new ColumnName(CATALOG, TABLE, ROW1));
        ColumnSelector colSelectorRight = new ColumnSelector(new ColumnName(OTHER_CATALOG, TABLE, ROW2));
        Relation relation1 = new Relation(colSelectorLeft, Operator.EQ, colSelectorRight);
        Relation relation2 = new Relation(colSelectorRight, Operator.EQ, colSelectorLeft);
        List<Relation> listRelations = Arrays.asList(relation1, relation2);

        ResultSet resultSet = new ResultSet();
        ColumnMetadata colMetadata = metaMetadata(CATALOG, TABLE, ROW1, ColumnType.INT);
        ColumnMetadata colMetadata2 = metaMetadata(CATALOG, TABLE, ROW2, ColumnType.INT);
        resultSet.setColumnMetadata(Arrays.asList(colMetadata, colMetadata2));
        PartialResults partialResults = new PartialResults(Operations.PARTIAL_RESULTS);
        partialResults.setResults(resultSet);

        List<Relation> orderedRelations = QueryPartialResultsUtils.getOrderedRelations(partialResults, listRelations);
        assertEquals(listRelations.size(), orderedRelations.size());
        assertEquals(colSelectorLeft.getName().getQualifiedName(), ((ColumnSelector) orderedRelations.get(0)
                .getLeftTerm()).getName().getQualifiedName());
        assertEquals(colSelectorLeft.getName().getQualifiedName(), ((ColumnSelector) orderedRelations.get(1)
                .getLeftTerm()).getName().getQualifiedName());

    }

    @Test
    public void createCellsFromResultSetTest() throws ExecutionException {

        ResultSet resultSet = new ResultSet();
        resultSet.setColumnMetadata(Arrays.asList(metaMetadata(CATALOG, TABLE, ROW1, ColumnType.INT),
                metaMetadata(CATALOG, TABLE, ROW2, ColumnType.VARCHAR)));
        resultSet.setRows(Arrays.asList(metaRow(ROW1, CELL1_VALUE1, ROW2, CELL1_VALUE2),
                metaRow(ROW1, CELL2_VALUE1, ROW2, CELL2_VALUE2)));

        // mockStatic(QueryPartialResultsUtils.class);
        // when(
        // QueryPartialResultsUtils.buildCellsFromRow(Matchers.any(Row.class), Matchers.anyString(),
        // Matchers.anyString(), Matchers.anyListOf(ColumnMetadata.class))).thenReturn(
        // new Cells(CATALOG + "." + TABLE, Cell.create(ROW1, CELL1_VALUE1), Cell.create(ROW2,
        // CELL1_VALUE2)));

        List<Cells> cellsFromResultSet = QueryPartialResultsUtils.createCellsFromResultSet(resultSet);

        assertEquals(2, cellsFromResultSet.size());
        Cells cell1 = cellsFromResultSet.get(0);
        Cells cell2 = cellsFromResultSet.get(1);
        assertEquals(CATALOG + "." + TABLE, cell1.getnameSpace());
        assertEquals(CATALOG + "." + TABLE, cell2.getnameSpace());
        boolean cellsOrdered = cell1.getInteger(ROW1).intValue() == CELL1_VALUE1;
        if (!cellsOrdered) {
            cell2 = cellsFromResultSet.get(0);
            cell1 = cellsFromResultSet.get(1);
        }
        assertEquals(CELL1_VALUE1, cell1.getInteger(ROW1));
        assertEquals(CELL1_VALUE2, cell1.getString(ROW2));
        assertEquals(CELL2_VALUE1, cell2.getInteger(ROW1));
        assertEquals(CELL2_VALUE2, cell2.getString(ROW2));

    }

    /**
     * @param catalog
     * @param table
     * @param row
     * @param i
     * @return ColumnMetadata
     */
    private ColumnMetadata metaMetadata(String catalog, String table, String row, ColumnType i) {

        return new ColumnMetadata(catalog + "." + table, catalog + "." + table + "." + row, i);
    }

    /**
     * @param col
     * @param col2
     * @params
     * @return Row
     */
    private Row metaRow(String col, Object cellValue, String col2, Object cellValue2) {
        Row row = new Row(col, new com.stratio.crossdata.common.data.Cell(cellValue));
        row.addCell(col2, new com.stratio.crossdata.common.data.Cell(cellValue2));
        return row;
    }

}
