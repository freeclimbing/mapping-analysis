package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.IdTypeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.utils.EdgeTypeJoinFunction;
import org.mappinganalysis.model.functions.preprocessing.utils.EqualTypesEdgeFilterFunction;
import org.mappinganalysis.model.functions.preprocessing.utils.VertexIdTypeTupleMapper;

/**
 * Type mismatch correction is part of the preprocessing.
 * Exclude edges where directly connected source and target vertices have different type property values.
 *
 * If needed, can be disabled in builder.
 *
 */
public class TypeMisMatchCorrection
    implements GraphAlgorithm<Long, ObjectMap, NullValue, Graph<Long, ObjectMap, NullValue>> {

  private final boolean isDisabled;
  private final ExecutionEnvironment env;

  public TypeMisMatchCorrection(boolean isDisabled, ExecutionEnvironment env) {
    this.isDisabled = isDisabled;
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, NullValue> run(Graph<Long, ObjectMap, NullValue> graph) throws Exception {
    if (isDisabled) {
      DataSet<IdTypeTuple> vertexIdAndTypeList = graph.getVertices()
          .flatMap(new VertexIdTypeTupleMapper());

      DataSet<Tuple4<Long, Long, String, String>> edgeTypes = graph.getEdges()
          .map(edge -> new Tuple4<>(edge.getSource(), edge.getTarget(), "", ""))
          .returns(new TypeHint<Tuple4<Long, Long, String, String>>() {})
          .join(vertexIdAndTypeList)
          .where(0).equalTo(0)
          .with(new EdgeTypeJoinFunction(0))
          .distinct()
          .join(vertexIdAndTypeList)
          .where(1)
          .equalTo(0)
          .with(new EdgeTypeJoinFunction(1))
          .distinct();

      DataSet<Edge<Long, NullValue>> edgesEqualType = edgeTypes
          .filter(new EqualTypesEdgeFilterFunction())
          .map(tuple -> new Edge<>(tuple.f0, tuple.f1, NullValue.getInstance()))
          .returns(new TypeHint<Edge<Long, NullValue>>() {})
          .distinct(0, 1);

      DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices()
          .runOperation(new IsolatedVertexRemover<>(edgesEqualType));

      return Graph.fromDataSet(resultVertices, edgesEqualType, env);

    }

    return graph;
  }

  /**
   * Used for building TypeMisMatchCorrection instance.
   */
  public static final class TypeMisMatchCorrectionBuilder {
    private ExecutionEnvironment env;
    private boolean isDisabled = false;

    public TypeMisMatchCorrectionBuilder disable() {
      this.isDisabled = true;
      return this;
    }

    public TypeMisMatchCorrectionBuilder setEnvironment(ExecutionEnvironment env) {
      this.env = env;
      return this;
    }

    /**
     * Creates class.
     * @return type mismatch correction class
     */
    public TypeMisMatchCorrection build() {
      if (env != null) {
        return new TypeMisMatchCorrection(isDisabled, env);
      } else {
        throw new IllegalArgumentException("Execution environment null");
      }
    }

  }
}
