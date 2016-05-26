package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

public class TypeGroupBy {
  /**
   * For a given graph, assign all vertices with no type to the component where the best similarity can be found.
   * @param graph input graph
   * @param processingMode if default, typeGroupBy is executed
   * @param maxIterations maximal count vertex centric iterations  @return graph where non-type vertices are assigned to best matching component
   */
  public Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph,
                                                   String processingMode, Integer maxIterations) throws Exception {
    if (processingMode.equals(Utils.DEFAULT_VALUE)) {
      VertexCentricConfiguration tbcParams = new VertexCentricConfiguration();
      tbcParams.setName("Type-based Cluster Generation Iteration");
      tbcParams.setDirection(EdgeDirection.ALL);

//      DataSet<Tuple2<Long, Integer>> preTypeGroupBy // low hash +  count
//          = Utils.writeVertexComponentsToHDFS(graph, Utils.CC_ID, "preTypeGroupBy");

      graph = graph.runVertexCentricIteration(
          new TypeGroupByVertexUpdateFunction(),
          new TypeGroupByMessagingFunction(), maxIterations, tbcParams);

//      DataSet<Tuple2<Long, Integer>> postTypeGroupBy // low hash +  count
//          = Utils.writeVertexComponentsToHDFS(graph, Utils.HASH_CC, "postTypeGroupBy");
//
//      final DataSet<Tuple3<Long, Integer, Integer>> logResult = preTypeGroupBy.rightOuterJoin(postTypeGroupBy)
//          .where(0)
//          .equalTo(0)
//          .with(new FlatJoinFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple3<Long, Integer, Integer>>() {
//            @Override
//            public void join(Tuple2<Long, Integer> left, Tuple2<Long, Integer> right,
//                             Collector<Tuple3<Long, Integer, Integer>> collector) throws Exception {
//              if (left == null) { // new component
//                collector.collect(new Tuple3<>(right.f0, 0, right.f1));
//              } else if (right.f1 - left.f1 != 4){
//                collector.collect(new Tuple3<>(right.f0, left.f1, right.f1));
//              }
//            }
//          });
//      Utils.writeToHdfs(logResult, "postTypeGroupBySingleChanges");

//      out.addTuples("postTypeGroupByAggChanges",
//          logResult.map(new MapFunction<Tuple3<Long,Integer, Integer>, Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> map(Tuple3<Long,Integer, Integer> tuple) throws Exception {
//              return new Tuple2<>(tuple.f1, 1);
//            }
//          }).groupBy(0).sum(1));

      return graph;
    } else {
      return graph;
    }
  }

  public String getName() {
    return TypeGroupBy.class.getName();
  }
}
