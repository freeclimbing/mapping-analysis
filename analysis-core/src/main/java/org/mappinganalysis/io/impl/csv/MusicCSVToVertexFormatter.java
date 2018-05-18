package org.mappinganalysis.io.impl.csv;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.MusicUtils;
import org.mappinganalysis.util.Utils;

/**
 * Musicbrainz vertex formatter.
 */
public class MusicCSVToVertexFormatter
    extends RichMapFunction<Tuple10<Long, Long, Long, String, String, String, String, String, String, String>,
        Vertex<Long, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(MusicCSVToVertexFormatter.class);
  /**
   * no reuse vertex, not every attribute gets new value
   */
  private Vertex<Long, ObjectMap> vertex = new Vertex<>();

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public Vertex<Long, ObjectMap> map(
      Tuple10<Long,  //0 TID --> vertex id
          Long,  //1 CID --> cluster id
          //          Long,  // CTID --> cluster intern id
          Long,  //2 SourceID, 1 to 5
          //          String,  //IGNORE - strange mix numbers and letters
          String,  //3 number - song number? sometimes letters involved
          String,  //4 title
          String,  //5 length, e.g., 4m 32sec, 432, 432000, 4.58
          String,  //6 artist
          String,  //7 album
          String,  //8 year, e.g., 2009, '09
          String> value)  //9 language
      throws Exception {
    vertex.setId(value.f0);
    ObjectMap properties = new ObjectMap(Constants.MUSIC);

    properties.setCcId(value.f1);
    properties.setLabel(value.f4);

    properties.setDataSource(value.f2.toString()); // int would be better, but data source is string
    properties.put(Constants.NUMBER, value.f3);

    Integer songLength = MusicUtils.fixSongLength(value.f5);
    if (songLength != null) {
      properties.setLength(songLength);
    }
    properties.put(Constants.ARTIST, value.f6);
    properties.put(Constants.ALBUM, value.f7);

    Integer year = MusicUtils.fixYear(value.f8);
    if (year != null) {
      properties.setYear(year);
    }
    properties.setLanguage(MusicUtils.fixLanguage(value.f9));

    properties.setArtistTitleAlbum(Utils.createSimpleArtistTitleAlbum(properties));

//    LOG.info("props: " + properties.toString());
    vertex.setValue(properties);
    return vertex;
  }

}