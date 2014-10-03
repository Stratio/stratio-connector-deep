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

import org.apache.spark.api.java.function.Function;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

public class DeepEquals implements Function<Cells, Boolean> {

  /**
   * Serial version UID.
   */
  private static final long serialVersionUID = -614353453245235444L;

  /**
   * Term to compare.
   */
  private Selector term;

  /**
   * Name of the field of the cell to compare.
   */
  private String field;

  /**
   * DeepEquals apply = filter to a field in a Deep Cell.
   * 
   * @param field Name of the field to check.
   * @param term Term to compare to.
   */
  public DeepEquals(String field, Selector term) {
    this.term = term;
    this.field = field;
  }

  @Override
  public Boolean call(Cells cells) {
    Object currentValue = cells.getCellByName(field).getCellValue();
    Object value = null;
    if(term instanceof StringSelector){
        value = ((StringSelector) term).getValue();
    }else if(term instanceof IntegerSelector){
         value = ((IntegerSelector) term).getValue();
    }


    return value.equals(currentValue);
  }
}
