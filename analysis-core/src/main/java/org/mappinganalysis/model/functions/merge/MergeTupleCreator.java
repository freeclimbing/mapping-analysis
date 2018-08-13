package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Create basic merge tuples for merge process, properties are transferred from
 * Gelly vertices.
 *
 * Care: Initial MergeTuples have some values set to avoid null pointer exceptions.
 * - therefore, dont use reuse tuples here
 */
public class MergeTupleCreator
    implements MapFunction<Vertex<Long, ObjectMap>, MergeTuple> {
  private static final Logger LOG = Logger.getLogger(MergeTupleCreator.class);
  private BlockingStrategy blockingStrategy;
  private DataDomain domain;
  private int blockingLength;

  /**
   * Default constructor
   * @param config config
   */
  public MergeTupleCreator(IncrementalConfig config) {
    this(config.getBlockingStrategy(),
        config.getDataDomain(),
        config.getBlockingLength());
  }

  /**
   * Default constructor.
   */
  public MergeTupleCreator(
      BlockingStrategy blockingStrategy,
      DataDomain domain,
      int blockingLength) {
    this.blockingStrategy = blockingStrategy;
    this.domain = domain;
    this.blockingLength = blockingLength;
  }

  @Override
  public MergeTuple map(Vertex<Long, ObjectMap> vertex) throws Exception {
    MergeTuple tuple = new MergeTuple();

    ObjectMap properties = vertex.getValue();
//    System.out.println("debug vertex props: " + properties.toString());

    // rly not needed? remvoe
    properties.setMode(domain);

    tuple.setId(vertex.getId());
    tuple.setLabel(properties.getLabel());

    String artistTitleAlbum = Constants.EMPTY_STRING;
    if (domain == DataDomain.MUSIC || domain == DataDomain.NC) {
      tuple.setAlbum(properties.getAlbum());
      tuple.setArtist(properties.getArtist());
      tuple.setLength(properties.getLength());
      tuple.setLang(properties.getLanguage());
      tuple.setNumber(properties.getNumber());
      tuple.setYear(properties.getYear());

      if (vertex.getValue().get(Constants.ARTIST_TITLE_ALBUM) == null) {
        artistTitleAlbum = Utils.createSimpleArtistTitleAlbum(vertex.getValue());
      } else {
        artistTitleAlbum = vertex.getValue().getArtistTitleAlbum();
      }

      tuple.setArtistTitleAlbum(artistTitleAlbum);

//      System.out.println("added ata: " + tuple.toString());
    }


    if (domain == DataDomain.GEOGRAPHY) {
      // TODO fix this, dirty
      if (properties.hasGeoPropertiesValid()) {
        Double latitude = properties.getLatitude();
        Double longitude = properties.getLongitude();
        tuple.setArtist(latitude.toString());
        tuple.setAlbum(longitude.toString());
      } else {
        tuple.setArtist("200");
        tuple.setAlbum("200");
      }

      tuple.setLength(Constants.EMPTY_INT);
      tuple.setYear(Constants.EMPTY_INT);
      tuple.setNumber(Constants.NO_VALUE);
      tuple.setLang(Constants.NO_VALUE);
//      tuple.setIntTypes(properties.getIntTypes());
    }

//    System.out.println("debug int src: " + properties.getIntDataSources());

    tuple.setIntSources(properties.getIntDataSources());
    properties.addClusterVertices(Sets.newHashSet(vertex.getId()));
    tuple.addClusteredElements(properties.getVerticesList());

    // replace
    String mode = null;
    if (domain.equals(DataDomain.GEOGRAPHY)) {
      mode = Constants.GEO;
    } else if (domain.equals(DataDomain.MUSIC)) {
      mode = Constants.MUSIC;
    } else if (domain.equals(DataDomain.NC)) {
      mode = Constants.NC;
    }

    if (domain == DataDomain.MUSIC) {
      tuple.setBlockingLabel(Utils.getBlockingKey(
          blockingStrategy,
          mode,
          artistTitleAlbum,
          blockingLength));
    } else if (domain == DataDomain.NC) {
      String ncBlocking = Utils.getNcBlockingLabel(
          properties.getArtist(), properties.getLabel(), blockingLength);

      tuple.setBlockingLabel(Utils.getBlockingKey(
          blockingStrategy,
          mode,
          ncBlocking,
          blockingLength));
    } else if (domain == DataDomain.GEOGRAPHY) {
      tuple.setBlockingLabel(Utils.getBlockingKey(
          blockingStrategy,
          Constants.GEO,
          properties.getLabel(),
          blockingLength));
//      System.out.println("MuTuCreator: " + properties.toString());
    } else {
      throw new IllegalArgumentException("dataDomain not supported: " + domain);
    }

//    LOG.info("created tuple: " + tuple.toString());
    return tuple;
  }
}
