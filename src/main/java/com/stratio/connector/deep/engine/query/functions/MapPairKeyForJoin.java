package com.stratio.connector.deep.engine.query.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.stratio.deep.commons.entity.Cells;

import scala.Tuple2;

/**
 * Created by dgomez on 3/10/14.
 */
public class MapPairKeyForJoin implements PairFlatMapFunction< Cells, String, Cells> {

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = -6865478919144416567L;

    public MapPairKeyForJoin() {
    }

    @Override
    public Iterable<Tuple2<String, Cells>> call(Cells t) throws Exception {
        return null;
    }
}
