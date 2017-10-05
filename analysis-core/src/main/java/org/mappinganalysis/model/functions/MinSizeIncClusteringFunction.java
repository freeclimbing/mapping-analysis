package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

import java.util.List;


/**
 * Incremental clustering based on minimum size of data sources.
 */
public class MinSizeIncClusteringFunction extends IncrementalClusteringFunction {
  private List<String> sources;
  private ExecutionEnvironment env;
  private static final Logger LOG = Logger.getLogger(MinSizeIncClusteringFunction.class);


  public MinSizeIncClusteringFunction(List<String> sources, ExecutionEnvironment env) {
    super();
    this.sources = sources;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> input) throws Exception {

    // TODO

    DataSet<Vertex<Long, ObjectMap>> dsCountVertices = input.getVertices()
        .map(vertex -> {
          vertex.getValue().setMode(Constants.GEO);
          vertex.getValue().setDataSourceEntityCount(1L);

          vertex.getValue().remove(Constants.LABEL);
          vertex.getValue().remove(Constants.LON);
          vertex.getValue().remove(Constants.LAT);
          vertex.getValue().remove(Constants.TYPE_INTERN);
          vertex.getValue().remove(Constants.DB_URL_FIELD);
          vertex.getValue().remove(Constants.TYPE);

          return vertex;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .groupBy(new KeySelector<Vertex<Long, ObjectMap>, Integer>() {
          @Override
          public Integer getKey(Vertex<Long, ObjectMap> value) throws Exception {
            return value.getValue().getIntDataSources();
          }
        })
        .reduce(new ReduceFunction<Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> reduce(
              Vertex<Long, ObjectMap> value1,
              Vertex<Long, ObjectMap> value2) throws Exception {
            long sum = value1.getValue().getDataSourceEntityCount()
                + value2.getValue().getDataSourceEntityCount();

            value1.getValue().setDataSourceEntityCount(sum);

            return value1;
          }
        });


    return Graph.fromDataSet(dsCountVertices, input.getEdges(), env);
//    return null;
  }
}
