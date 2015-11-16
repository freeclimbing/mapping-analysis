package org.mappinganalysis.model.functions;import com.google.common.collect.Maps;import org.apache.flink.api.common.functions.JoinFunction;import org.apache.flink.graph.Edge;import org.apache.flink.graph.Triplet;import org.apache.flink.graph.Vertex;import org.mappinganalysis.model.FlinkVertex;import java.util.Map;/** * Join the result of two different matching results. */public class JoinSimilarityValueFunction    implements JoinFunction<Triplet<Long, FlinkVertex, Map<String, Object>>,    Triplet<Long, FlinkVertex, Map<String, Object>>,    Triplet<Long, FlinkVertex, Map<String, Object>>> {  @Override  public Triplet<Long, FlinkVertex, Map<String, Object>> join(      Triplet<Long, FlinkVertex, Map<String, Object>> t1,      Triplet<Long, FlinkVertex, Map<String, Object>> t2) throws Exception {    Vertex<Long, FlinkVertex> source;    Vertex<Long, FlinkVertex> target;    if (t1 != null) {      source = t1.getSrcVertex();      target = t1.getTrgVertex();    } else {      source = t2.getSrcVertex();      target = t2.getTrgVertex();    }    Map<String, Object> result = Maps.newHashMap();    if (t1 != null) {      result.putAll(t1.getEdge().getValue());    }    if (t2 != null) {      result.putAll(t2.getEdge().getValue());    }    return new Triplet<>(        source,        target,        new Edge<>(            source.getId(),            target.getId(),            result));  }}