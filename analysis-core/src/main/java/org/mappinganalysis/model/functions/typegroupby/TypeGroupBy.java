package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.NeighborTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

public class TypeGroupBy {
  private static final Logger LOG = Logger.getLogger(TypeGroupBy.class);

  /**
   * For a given graph, assign all vertices with no type to the component where the best similarity can be found.
   * @param graph input graph
   * @param processingMode if default, typeGroupBy is executed
   * @param maxIterations maximal count vertex centric iterations
   * @return graph where non-type vertices are assigned to best matching component
   */
  public static Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph,
                                                   String processingMode,
                                                   Integer maxIterations,
                                                   ExecutionEnvironment env, ExampleOutput out) throws Exception {
    // start preprocessing
    // sync begin
//    DataSet<Vertex<Long, ObjectMap>> tmpVertices = graph.getVertices().filter(value -> true);
//    DataSet<Edge<Long, ObjectMap>> edges = graph.getEdges().filter(value -> true);
//    tmpVertices = tmpVertices.map(new PrintVertices(false, "preprocTGB1"));
//    graph = Graph.fromDataSet(tmpVertices, edges, env);
    // sync end

    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices()
        .map(new AddShadingTypeMapFunction())
        .groupBy(new CcIdKeySelector())
        .reduceGroup(new HashCcIdOverlappingFunction());
    graph = Graph.fromDataSet(vertices, graph.getEdges(), env);
    // end preprocessing

    /**
     * Start typed grouping
     */
    final DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
        .getTransitiveClosureEdges(graph.getVertices(), new CcIdKeySelector());
    final DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
        .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
            Constants.SIM_GEO_LABEL_STRATEGY);

    graph = Graph.fromDataSet(graph.getVertices(), simEdges, env);

    final DataSet<NeighborTuple> neighborSimTypes = graph
        .groupReduceOnNeighbors(new NeighborTupleCreator(), EdgeDirection.ALL);

    final DataSet<NeighborTuple> maxTypedSimValues = getMaxNeighborSims(neighborSimTypes);

    // all tuples minus max tuple ids with type
    DataSet<NeighborTuple> noTypedNeighborsCandidates = neighborSimTypes
        .leftOuterJoin(maxTypedSimValues)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<NeighborTuple,
            NeighborTuple, NeighborTuple>() {
          @Override
          public void join(NeighborTuple first, NeighborTuple second,
                           Collector<NeighborTuple> out) throws Exception {
            if (second == null) {
//                LOG.info("noTypeCandidate: " + first.toString());
              out.collect(first);
            }
          }
        });

    DataSet<Vertex<Long, ObjectMap>> noTypedNeighbors = noTypedNeighborsCandidates
        .groupBy(0)
        .min(3)
        .leftOuterJoin(vertices)
        .where(0)
        .equalTo(0)
        .with((left, right) -> {
          if (right.getValue().getHashCcId() < left.getCompId()) {
            right.getValue().put(Constants.HASH_CC, left.getCompId());
          }
          return right;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> typedNeighbors = maxTypedSimValues
        .leftOuterJoin(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with((left, right) -> {
          right.getValue().put(Constants.HASH_CC, left.getCompId());
          return right;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {
        });

    DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
        .leftOuterJoin(noTypedNeighbors.union(typedNeighbors))
        .where(0)
        .equalTo(0)
        .with((unchanged, updated) -> {
          if (updated == null) {
//              LOG.info("final: unchanged: " + unchanged.toString());
            return unchanged;
          } else {
//              LOG.info("final: unchanged: " + unchanged.toString() + " updated: " + updated.toString());
            return updated;
          }
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});


    graph = Graph.fromDataSet(newVertices, graph.getEdges(), env);

    return graph;
  }

  private static DataSet<NeighborTuple> getMaxNeighborSims(
      DataSet<NeighborTuple> neighborSimTypes) {

    final DataSet<NeighborTuple> typeVals = neighborSimTypes
        .filter(value -> !value.getTypes().contains(Constants.NO_TYPE));

    return typeVals
            .groupBy(0).max(1)
            .leftOuterJoin(typeVals)
            .where(0,1)
            .equalTo(0,1)
            .with((left, right) -> right)
            .returns(new TypeHint<NeighborTuple>() {})
            .groupBy(0)
            .min(3);
  }

}
