package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.LinkFilterFunction;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.utils.EdgeSourceSimTuple;
import org.mappinganalysis.model.functions.preprocessing.utils.LinkSelectionWithCcIdFunction;
import org.mappinganalysis.model.functions.preprocessing.utils.NeighborEqualDataSourceFunction;
import org.mappinganalysis.util.Constants;

import java.util.List;

/**
 * Actual implementation for basic link filter.
 *
 * preprocessing: currently in use simple 1:n removal
 * TODO grouping based on ccId still used for creating independent blocks, how to avoid?
 */
public class BasicLinkFilterFunction
    extends LinkFilterFunction {
  private static final Logger LOG = Logger.getLogger(BasicLinkFilterFunction.class);

  private List<String> sources;
  private Boolean removeIsolatedVertices;
  private ExecutionEnvironment env;

  BasicLinkFilterFunction(
      List<String> sources,
      Boolean removeIsolatedVertices,
      ExecutionEnvironment env) {
    this.sources = sources;
    this.removeIsolatedVertices = removeIsolatedVertices;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph)
      throws Exception {
    // first cc needed for neighbor grouping - needed for bigger data sets
    graph = graph.run(new ConnectedComponentIdAdder<>(env));
    String mode = getModeFromSources(sources);

    // EdgeSourceSimTuple(ccid, edge src, edge trg, vertex ont, neighbor ont, EdgeSim)
    DataSet<EdgeSourceSimTuple> neighborTuples = graph
        .groupReduceOnNeighbors(new NeighborEqualDataSourceFunction(mode), EdgeDirection.OUT);

    DataSet<Tuple2<Long, Long>> edgeTuples = neighborTuples.groupBy(0) // cc id
        .sortGroup(5, Order.DESCENDING) // sim
        .sortGroup(1, Order.ASCENDING) // src id
        .sortGroup(2, Order.ASCENDING) // trg id
        .reduceGroup(new LinkSelectionWithCcIdFunction());

    DataSet<Edge<Long, ObjectMap>> newEdges = edgeTuples.join(graph.getEdges())
        .where(0, 1)
        .equalTo(0, 1)
        .with((tuple, edge) -> edge)
        .returns(new TypeHint<Edge<Long, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> resultVertices;
    if (removeIsolatedVertices) {
      resultVertices = graph.getVertices()
          .runOperation(new IsolatedVertexRemover<>(newEdges));
    } else {
      resultVertices = graph.getVertices();
    }

    return Graph.fromDataSet(resultVertices, newEdges, env)
        .run(new ConnectedComponentIdAdder<>(env)); // CC needed
  }

  private String getModeFromSources(List<String> sources) {
    if (sources.iterator().next().startsWith("geco")) {
      return Constants.NC;
    } else if (sources.iterator().next().startsWith("http")) {
      return Constants.GEO;
    } else {
      return Constants.MUSIC;
    }
  }
}
