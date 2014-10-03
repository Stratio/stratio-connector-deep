package com.stratio.connector.deep.engine.query.functions;

import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.stratio.deep.commons.entity.Cells;

/**
 * Created by dgomez on 2/10/14.
 */
public class MapKeysForJoin implements FlatMapFunction <Cells, String> {

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -6677647619144416567L;

    /**
     * Map key.
     */
    private List<String> keys;

    /**
     * MapKeyForJoin maps a field in a Cell.
     *
     * @param keys Field to map
     */
    public MapKeysForJoin(List<String> keys) {
        this.keys = keys;
    }

    @Override
    public Iterable<String> call(Cells cells) throws Exception {
        return null;
    }
}
