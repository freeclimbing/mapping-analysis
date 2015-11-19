package org.mappinganalysis.model.functions;import com.google.common.collect.Maps;import org.apache.flink.api.common.functions.MapFunction;import org.apache.flink.graph.Edge;import org.apache.flink.graph.Triplet;import org.apache.flink.types.NullValue;import org.mappinganalysis.model.FlinkVertex;import org.simmetrics.StringMetric;import org.simmetrics.metrics.CosineSimilarity;import org.simmetrics.metrics.StringMetrics;import org.simmetrics.tokenizers.Tokenizers;import java.util.Map;import static org.simmetrics.builders.StringMetricBuilder.with;/** * Return similarity 1f if labels of two resources are equal. */public class SimilarTripletExtractor implements MapFunction<Triplet<Long, FlinkVertex, NullValue>,    Triplet<Long, FlinkVertex, Map<String, Object>>> {  @Override  public Triplet<Long, FlinkVertex, Map<String, Object>> map(Triplet<Long, FlinkVertex, NullValue> triplet) throws Exception {    Map<String, Object> srcProps = triplet.getSrcVertex().getValue().getProperties();    String srcLabel = srcProps.containsKey("label") ? srcProps.get("label").toString() : "null";    Map<String, Object> trgProps = triplet.getTrgVertex().getValue().getProperties();    String trgLabel = trgProps.containsKey("label") ? trgProps.get("label").toString() : "null";    StringMetric metric =        with(new CosineSimilarity<String>())            .tokenize(Tokenizers.qGram(3))            .build();    float similarity = 0f;    if (!srcLabel.equals("null") && !trgLabel.equals("null")) {      similarity = metric.compare(srcLabel.toLowerCase(), trgLabel.toLowerCase());    }    Map<String, Object> property = Maps.newHashMap();    property.put("trigramSim", similarity);    return new Triplet<>(        triplet.getSrcVertex(),        triplet.getTrgVertex(),        new Edge<>(            triplet.getSrcVertex().getId(),            triplet.getTrgVertex().getId(),            property));  }}