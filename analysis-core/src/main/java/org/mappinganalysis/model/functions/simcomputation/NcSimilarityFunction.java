package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.ops.SinglePropertySimilarity;
import org.mappinganalysis.util.Constants;

/**
 * North Carolina voter dataset similarity function
 */
public class NcSimilarityFunction
    extends SimilarityFunction<Triplet<Long, ObjectMap, NullValue>, Triplet<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(NcSimilarityFunction.class);

  NcSimilarityFunction(String metric) {
    this.metric = metric;
  }

  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(
      Triplet<Long, ObjectMap, NullValue> triplet) throws Exception {

    EdgeObjectMapTriplet result = new EdgeObjectMapTriplet(triplet);
    result.runOperation(new SinglePropertySimilarity(Constants.LABEL, metric))
        .runOperation(new SinglePropertySimilarity(Constants.ARTIST, metric))
        .runOperation(new SinglePropertySimilarity(Constants.ALBUM, metric))
        .runOperation(new SinglePropertySimilarity(Constants.NUMBER, metric));

    return result;
  }
}
