package org.mappinganalysis.model.functions.simcomputation;

import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
/**
 * Music edge similarity function, {@value Constants#ARTIST_TITLE_ALBUM} used
 * for better results.
 */
public class MusicTripletSimilarityFunction
    extends SimilarityFunction<MergeTriplet, MergeTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MusicTripletSimilarityFunction.class);

  public MusicTripletSimilarityFunction(String metric) {
    this.metric = metric;
  }

  @Override
  public MergeTriplet map(MergeTriplet triplet) throws Exception {
//    System.out.println(triplet.getSrcTuple().getArtistTitleAlbum() + " second: " + triplet.getTrgTuple().getArtistTitleAlbum());
    Double similarity = Utils.getSimilarityAndSimplifyForMetric(
        triplet.getSrcTuple().getArtistTitleAlbum(),
        triplet.getTrgTuple().getArtistTitleAlbum(),
        metric);
    if (similarity != null) {
      triplet.setSimilarity(similarity);
    } else {
      throw new NullPointerException("similarity should not be null");
    }

    return triplet;
  }
}
