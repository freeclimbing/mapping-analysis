package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.typegroupby.HashCcIdOverlappingFunction;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * type overlap - this is not in typeGroupBy because of some testing issues
 * only relevant for geographic domain (or other semantic type data sets)
 */
public class TypeOverlapCcCreator
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private DataDomain domain;
  private ExecutionEnvironment env;

  /**
   * Formerly part of TypeGroupBy.
   * Based on given connected components inforamtion, create new groups of vertices
   * using the type information, e.g., "Mountain -- Mountain, Island -- Island" into
   * one component
   */
  public TypeOverlapCcCreator(DataDomain domain, ExecutionEnvironment env) {
    this.domain = domain;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices()
        .map(new AddShadingTypeMapFunction())
        .groupBy(new CcIdKeySelector())
        .reduceGroup(new HashCcIdOverlappingFunction(domain));

    return Graph.fromDataSet(vertices, graph.getEdges(), env);  }
}
