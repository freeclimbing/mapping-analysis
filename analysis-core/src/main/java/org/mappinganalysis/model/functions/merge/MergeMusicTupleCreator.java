package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 */
public class MergeMusicTupleCreator
    implements MapFunction<Vertex<Long, ObjectMap>, MergeMusicTuple> {
  private static final Logger LOG = Logger.getLogger(MergeMusicTupleCreator.class);
  private String mode = null;
  private BlockingStrategy blockingStrategy;
  private int blockingLength;

  public MergeMusicTupleCreator(BlockingStrategy blockingStrategy) {
    this.blockingStrategy = blockingStrategy;
    this.mode = Constants.MUSIC;
  }

  public MergeMusicTupleCreator(IncrementalConfig config) {
    this(config.getBlockingStrategy(),
        config.getDataDomain(),
        config.getBlockingLength());
  }

  /**
   * Test only
   */
  public MergeMusicTupleCreator() {
    blockingStrategy = BlockingStrategy.STANDARD_BLOCKING;
    this.mode = Constants.MUSIC;
  }

  /**
   * old: NC Benchmark parts and MergeExecution only
   */
  @Deprecated
  public MergeMusicTupleCreator(
      BlockingStrategy blockingStrategy,
      DataDomain domain) {
    this(blockingStrategy, domain, 4);
  }

  public MergeMusicTupleCreator(
      BlockingStrategy blockingStrategy,
      DataDomain domain,
      int blockingLength) {
    this.blockingStrategy = blockingStrategy;
    this.blockingLength = blockingLength;
    // TODO fix remove mode
    if (domain == DataDomain.MUSIC) {
      this.mode = Constants.MUSIC;
    } else if (domain == DataDomain.NC) {
      this.mode = Constants.NC;
    } else {
      this.mode = null;
    }
  }

  @Override
  public MergeMusicTuple map(Vertex<Long, ObjectMap> vertex) throws Exception {
    MergeMusicTuple tuple = new MergeMusicTuple();

    ObjectMap properties = vertex.getValue();
    properties.setMode(mode);

    tuple.setId(vertex.getId());
    tuple.setLabel(properties.getLabel());

    tuple.setAlbum(properties.getAlbum());
    tuple.setArtist(properties.getArtist());
    tuple.setLength(properties.getLength());
    tuple.setLang(properties.getLanguage());
    tuple.setNumber(properties.getNumber());
    tuple.setYear(properties.getYear());

    tuple.setIntSources(properties.getIntDataSources());
    properties.addClusterVertices(Sets.newHashSet(vertex.getId()));
    tuple.addClusteredElements(properties.getVerticesList());
    String artistTitleAlbum = Utils.createSimpleArtistTitleAlbum(vertex.getValue());

    if (mode.equals(Constants.MUSIC)) {
      tuple.setBlockingLabel(Utils.getBlockingKey(
          blockingStrategy,
          mode,
          artistTitleAlbum,
          blockingLength));
    } else {
      String ncBlocking = Utils.getNcBlockingLabel(
          properties.getArtist(), properties.getLabel(), blockingLength);

      tuple.setBlockingLabel(Utils.getBlockingKey(
          blockingStrategy,
          Constants.NC,
          ncBlocking,
          blockingLength));
    }

    tuple.setArtistTitleAlbum(artistTitleAlbum);
    return tuple;
  }
}
