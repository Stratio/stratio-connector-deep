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

package com.stratio.connector.deep.engine.query.transformation;

import java.util.List;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.deep.commons.entity.Cells;

/**
 * Spark function that joins the fields of two Cells as a result of InnerJoin.
 */
public class JoinCells implements Function<Tuple2<List<Object>, Tuple2<Cells, Cells>>, Cells> {

    private static final long serialVersionUID = 4534397129761833793L;

    public JoinCells() {

    }

    @Override
    public Cells call(Tuple2<List<Object>, Tuple2<Cells, Cells>> result) {

        Cells left = result._2()._1();
        Cells right = result._2()._2();

        Cells joinedCells = new Cells();
        joinedCells.addAll(left.getInternalCells());
        joinedCells.addAll(right.getInternalCells());

        return joinedCells;
    }
}
