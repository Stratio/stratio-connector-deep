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

package com.stratio.connector.deep.mappings.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.stratio.connector.deep.mappings.trasfer.ColumnInfo;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.statements.structures.selectors.SelectorGroupBy;
import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;
import com.stratio.meta.common.statements.structures.selectors.SelectorMeta;
import com.stratio.meta.core.structures.Selection;
import com.stratio.meta.core.structures.SelectionSelectors;
import com.stratio.meta2.common.metadata.ColumnType;

public final class DeepUtils {

  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(DeepUtils.class);

  /**
   * Private class constructor as all methods are static.
   */
  private DeepUtils() {

  }

  /**
   * Retrieve fields in selection clause.
   * 
   * @param selection Selection
   * @return Array of fields in selection clause or null if all fields has been selected
   */
  public static List<ColumnInfo> retrieveSelectorAggegationFunctions(Selection selection) {

    // Retrieve aggretation function column names
    List<ColumnInfo> columnsSet = new ArrayList<>();
    if (selection instanceof SelectionSelectors) {
      SelectionSelectors sSelectors = (SelectionSelectors) selection;
      for (int i = 0; i < sSelectors.getSelectors().size(); ++i) {

        SelectorMeta selectorMeta = sSelectors.getSelectors().get(i).getSelector();
        if (selectorMeta instanceof SelectorGroupBy) {
          SelectorGroupBy selGroup = (SelectorGroupBy) selectorMeta;
          SelectorIdentifier selId = (SelectorIdentifier) selGroup.getParam();
          columnsSet.add(new ColumnInfo(selId.getTable().getName(), selId.getField(), selGroup
              .getGbFunction()));
        }
      }
    }

    return columnsSet;
  }
}
