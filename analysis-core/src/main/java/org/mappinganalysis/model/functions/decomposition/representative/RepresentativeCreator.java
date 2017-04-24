package org.mappinganalysis.model.functions.decomposition.representative;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;

/**
 * Create representatives based on hash component ids for each vertex in a graph.
 */
public class RepresentativeCreator
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private DataSet<Vertex<Long, ObjectMap>> vertices;
  private DataDomain domain;

  public RepresentativeCreator(DataDomain domain) {
    this.domain = domain;
  }

  @Override
  public void setInput(DataSet<Vertex<Long, ObjectMap>> vertices) {
    this.vertices = vertices;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> createResult() {
    if (domain == DataDomain.MUSIC) {
      return vertices
          .groupBy(new HashCcIdKeySelector())
          .reduceGroup(new MajorityPropertiesGroupReduceFunction());
    } else if (domain == DataDomain.GEOGRAPHY) {
      return vertices
          .groupBy(new HashCcIdKeySelector())
          .reduceGroup(new MajorityPropertiesGroupReduceFunction());
    } else {
      return null;
    }
  }
}
