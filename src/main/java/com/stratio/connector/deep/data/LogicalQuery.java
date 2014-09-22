/**
 * 
 */
package com.stratio.connector.deep.data;

import java.util.List;
import java.util.concurrent.Future;

import org.apache.spark.rdd.RDD;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Project;

/**
 * @author Óscar Puertas
 *
 */
public class LogicalQuery {
  
  private final Project projection;
  
  private final List<Filter> filters;
  
  private final RDD<Cells> initialRdd;
  
  private Future<RDD<Cells>> rdd;

  public LogicalQuery(Project projection, List<Filter> filters) {
    this.projection = projection;
    this.filters = filters;
    this.initialRdd = null;
  }

  public LogicalQuery(Project projection, List<Filter> filters, RDD<Cells> initialRdd) {
    this.projection = projection;
    this.filters = filters;
    this.initialRdd = initialRdd;
  }
  
  public Project getProjection() {
    return projection;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public RDD<Cells> getInitialRdd() {
    return initialRdd;
  }

  public Future<RDD<Cells>> getRdd() {
    return rdd;
  }

  public void setRdd(Future<RDD<Cells>> rdd) {
    this.rdd = rdd;
  }
  
  public boolean hasInitialRdd() {
    return initialRdd != null;
  }
}