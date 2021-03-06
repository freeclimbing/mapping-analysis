package org.mappinganalysis.model.functions.decomposition.typegroupby;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.NeighborTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * ATTENTION: TypeGroupBy works on HashCcIds, dont confuse with CcIds.
 *
 * Actually different semantic types are needed in dataset,
 * so currently only supported for geographic domain.
 */
public class TypeGroupBy
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(TypeGroupBy.class);
  private ExecutionEnvironment env;

  public TypeGroupBy(ExecutionEnvironment env) {
    this.env = env;
  }

  /**
   * TODO rewrite
   * For a given graph, assign all vertices with no type to the component where the best similarity can be found.
   * @param graph input graph
   * @return graph where non-type vertices are assigned to best matching component
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph)
      throws Exception {
    final DataSet<NeighborTuple> neighborTypeOptionsForVerticesWoType = graph
        .groupReduceOnNeighbors(new NeighborTupleCreator(), EdgeDirection.ALL);

    final DataSet<NeighborTuple> maxTypedSimValues = getMaxNeighborSims(neighborTypeOptionsForVerticesWoType);

    // all tuples minus max tuple ids with type
    DataSet<Vertex<Long, ObjectMap>> noTypeVerticesWithNoTypeNeighbors = neighborTypeOptionsForVerticesWoType
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
        .distinct(0)
        // for groups of untyped vertices, select the lowest cc id to represent the group
        .leftOuterJoin(graph.getVertices())
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

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices()
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

    return Graph.fromDataSet(resultVertices, graph.getEdges(), env);
  }

  /**
   * For each no type vertex, get all neighbors with maximal similarity.
   * If there are several options, take neighbor with lowest cc value.
   * If neighbor has no type, it is not taken as result.
   */
  private static DataSet<NeighborTuple> getMaxNeighborSims(
      DataSet<NeighborTuple> neighborSimTypes) {
    final DataSet<NeighborTuple> typeVals = neighborSimTypes
        .filter(value -> !value.getTypes().contains(Constants.NO_TYPE));

    return typeVals
            .groupBy(0).max(1) // get maximum value for vertex id
            .join(typeVals)
            .where(0,1) // vertex id, edge sim
            .equalTo(0,1)
            .with((left, right) -> right)
            .returns(new TypeHint<NeighborTuple>() {})
            .groupBy(0)
            .min(3); // hash cc
  }
}
