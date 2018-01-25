package org.mappinganalysis.model.functions.decomposition.representative;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;

/**
 * Create representatives based on hash component ids for each vertex in a graph.
 */
public class RepresentativeCreatorMultiMerge
    implements CustomUnaryOperation<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(RepresentativeCreatorMultiMerge.class);

  private DataSet<Vertex<Long, ObjectMap>> vertices;
  private DataDomain domain;

  public RepresentativeCreatorMultiMerge(DataDomain domain) {
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
          .reduceGroup(new MusicMajorityPropertiesGroupReduceFunction());
    } else if (domain == DataDomain.NC) {
      return vertices
          .groupBy(new HashCcIdKeySelector())
          .reduceGroup(new NcMajorityPropertiesGroupReduceFunction());
    } else if (domain == DataDomain.GEOGRAPHY) {
      return vertices
          .groupBy(new HashCcIdKeySelector())
          .reduceGroup(new GeographicMajorityPropertiesGroupReduceFunction());
    } else {
      return null;
    }
  }
}
