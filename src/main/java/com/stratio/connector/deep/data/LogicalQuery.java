/**
 * 
 */
package com.stratio.connector.deep.data;

import java.util.List;

import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;

/**
 * @author Óscar Puertas
 *
 */
public class LogicalQuery {
  
  private List<Select> selectors;
  
  private List<Project> projections;
  
  private List<Filter> filters;

  public List<Select> getSelectors() {
    return selectors;
  }

  public void setSelectors(List<Select> selectors) {
    this.selectors = selectors;
  }

  public List<Project> getProjections() {
    return projections;
  }

  public void setProjections(List<Project> projections) {
    this.projections = projections;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public void setFilters(List<Filter> filters) {
    this.filters = filters;
  }
}
