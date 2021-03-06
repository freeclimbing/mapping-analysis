package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.primitives.Doubles;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.util.Set;

/**
 * Basic edge similarity function (geographic)
 */
public class GeoSimilarityFunction
    extends SimilarityFunction<Triplet<Long, ObjectMap, NullValue>, Triplet<Long, ObjectMap, ObjectMap>>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(GeoSimilarityFunction.class);

  private final String mode;
  private final double maxDistInMeter;

  public GeoSimilarityFunction(String metric, String mode, double maxDistInMeter) {
    this.metric = metric;
    this.mode = mode;
    this.maxDistInMeter = maxDistInMeter;
  }


  @Override
  public Triplet<Long, ObjectMap, ObjectMap> map(Triplet<Long, ObjectMap, NullValue> triplet)
      throws Exception {
    ObjectMap srcProps = triplet.getSrcVertex().getValue();
    ObjectMap trgProps = triplet.getTrgVertex().getValue();
    Triplet<Long, ObjectMap, ObjectMap> result = initResultTriplet(triplet);

    Double labelSimilarity = Utils.getSimilarityAndSimplifyForMetric(
        srcProps.getLabel(),
        trgProps.getLabel(),
        metric);
    result.getEdge().getValue().put(Constants.SIM_LABEL, labelSimilarity);

    // TODO remove dirty solution, check if needed
    if (srcProps.containsKey(Constants.ARTIST) && srcProps.containsKey(Constants.ALBUM)) {
      System.out.println(srcProps.toString());
      srcProps.setGeoProperties(
          Doubles.tryParse(srcProps.getArtist()),
          Doubles.tryParse(srcProps.getAlbum()));
    }
    if (trgProps.containsKey(Constants.ARTIST) && trgProps.containsKey(Constants.ALBUM)) {
      trgProps.setGeoProperties(
          Doubles.tryParse(trgProps.getArtist()),
          Doubles.tryParse(trgProps.getAlbum()));
    }

    Double geoSimilarity = Utils.getGeoSimilarity(srcProps.getLatitude(),
        srcProps.getLongitude(),
        trgProps.getLatitude(),
        trgProps.getLongitude());
    if (geoSimilarity != null) {
      result.getEdge().getValue().setGeoSimilarity(geoSimilarity);
    }

    if (mode.equals(Constants.GEO)) {
      result = addTypeSimilarity(result);
    }

    return result;
  }

  /**
   * add type similarity
   */
  private Triplet<Long, ObjectMap, ObjectMap> addTypeSimilarity(
      Triplet<Long, ObjectMap, ObjectMap> triplet) {
    Set<String> srcTypes = triplet.getSrcVertex().getValue().getTypes(Constants.TYPE_INTERN);
    Set<String> trgTypes = triplet.getTrgVertex().getValue().getTypes(Constants.TYPE_INTERN);

    if (!Utils.hasEmptyType(srcTypes, trgTypes)) {
      triplet.getEdge()
          .getValue()
          .put(Constants.SIM_TYPE, Utils.getTypeSim(srcTypes, trgTypes));
    }

    return triplet;
  }

  private Triplet<Long, ObjectMap, ObjectMap> initResultTriplet(
      Triplet<Long, ObjectMap, NullValue> triplet) {
    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            new ObjectMap())); // edge
  }
}
