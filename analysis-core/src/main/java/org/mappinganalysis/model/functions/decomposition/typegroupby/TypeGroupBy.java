package org.mappinganalysis.model.functions.decomposition.typegroupby;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.*;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.NeighborTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

public class TypeGroupBy
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(TypeGroupBy.class);
  private ExecutionEnvironment env;

  public TypeGroupBy(ExecutionEnvironment env) {
    this.env = env;
  }

  /**
   * For a given graph, assign all vertices with no type to the component where the best similarity can be found.
   * @param graph input graph
   * @return graph where non-type vertices are assigned to best matching component
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph)
      throws Exception {
    // start preprocessing
    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices()
        .map(new AddShadingTypeMapFunction())
        .groupBy(new CcIdKeySelector())
        .reduceGroup(new HashCcIdOverlappingFunction());
    graph = Graph.fromDataSet(vertices, graph.getEdges(), env);
    // end preprocessing

    /**
     * Start typed grouping
     */
    final DataSet<NeighborTuple> allTypeOptionsForUntyped = graph
        .groupReduceOnNeighbors(new NeighborTupleCreator(), EdgeDirection.ALL);

    final DataSet<NeighborTuple> maxTypedSimValues = getMaxNeighborSims(allTypeOptionsForUntyped);

    // all tuples minus max tuple ids with type
    DataSet<Vertex<Long, ObjectMap>> noTypeVerticesWithNoTypeNeighbors = allTypeOptionsForUntyped
        .leftOuterJoin(maxTypedSimValues)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<NeighborTuple, NeighborTuple, NeighborTuple>() {
          @Override
          public void join(NeighborTuple first, NeighborTuple second,
                           Collector<NeighborTuple> out) throws Exception {
            if (second == null) {
//              LOG.info("noT " + first);
              out.collect(first);
            }
          }
        })
//        .groupBy(0)
//        .min(3)
        .distinct(0)
        .leftOuterJoin(vertices)
        .where(0)
        .equalTo(0)
        .with((left, right) -> {
          if (right.getValue().getHashCcId() < left.getCompId()) {
//            LOG.info("add: " + right.toString() + " leftcompid: " + left.getCompId());
            right.getValue().setHashCcId(left.getCompId());
          }
//          LOG.info("end: " + right.toString() + " left: " + left.toString());
          return right;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> typedNeighbors = maxTypedSimValues
        .joinWithHuge(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with((left, right) -> {
          right.getValue().setHashCcId(left.getCompId());
          return right;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
        .leftOuterJoin(noTypeVerticesWithNoTypeNeighbors.union(typedNeighbors))
        .where(0)
        .equalTo(0)
        .with((unchanged, updated) -> {
          if (updated == null) {
            return unchanged;
          } else {
            return updated;
          }
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    return Graph.fromDataSet(newVertices, graph.getEdges(), env);
  }

  /**
   * helper method
   */
  private static DataSet<NeighborTuple> getMaxNeighborSims(
      DataSet<NeighborTuple> neighborSimTypes) {
    final DataSet<NeighborTuple> typeVals = neighborSimTypes
        .filter(value -> !value.getTypes().contains(Constants.NO_TYPE));

    return typeVals
            .groupBy(0).max(1)
            .join(typeVals)
            .where(0,1)
            .equalTo(0,1)
            .with((left, right) -> right)
            .returns(new TypeHint<NeighborTuple>() {})
            .groupBy(0)
            .min(3);
  }
}
