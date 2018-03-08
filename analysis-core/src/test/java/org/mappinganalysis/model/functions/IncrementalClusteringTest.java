package org.mappinganalysis.model.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.incremental.RepresentativeCreator;
import org.mappinganalysis.model.functions.merge.DualMergeGeographyMapper;
import org.mappinganalysis.model.functions.merge.FinalMergeGeoVertexCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IncrementalClusteringTest {
  private static final Logger LOG = Logger.getLogger(IncrementalClusteringTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  private final Set<Long> GN_EIGHTY = Sets.newHashSet(6110L, 1040L, 5486L, 5373L, 5998L, 4002L, 2988L, 5673L, 545L, 1159L, 6423L, 5340L, 4298L, 5864L, 5537L, 6032L, 5109L, 4177L, 1266L, 5418L, 3075L, 3311L, 7422L, 2692L, 6663L, 3899L, 6115L, 6630L, 1680L, 1817L, 5324L, 720L, 4842L, 1774L, 3309L, 5513L, 5822L, 81L, 3971L, 417L, 4216L, 7066L, 2703L, 7236L, 7373L, 5522L, 4650L, 6603L, 2092L, 7055L, 5084L, 5512L, 5501L, 213L, 7253L, 5415L, 4106L, 1672L, 1299L, 6560L, 3666L, 6585L, 1860L, 5552L, 906L, 1696L, 3409L, 987L, 358L, 1314L, 1392L, 1322L, 2069L, 3997L, 7L, 5747L, 1838L, 4621L, 3494L, 1923L, 3765L, 7389L, 2412L, 6519L, 6479L, 4351L, 7180L, 2032L, 5232L, 4213L, 181L, 4833L, 5235L, 6854L, 7088L, 6865L, 6840L, 3741L, 1094L, 3483L, 7529L, 2111L, 6203L, 3105L, 1243L, 5338L, 7330L, 1766L, 1655L, 6063L, 5548L, 2012L, 5040L, 5516L, 7322L, 6658L, 1196L, 3251L, 7435L, 5222L, 4105L, 3175L, 5205L, 6051L, 1198L, 1465L, 5603L, 1471L, 3662L, 7309L, 7245L, 4727L, 3545L, 4718L, 2618L, 4417L, 4042L, 5863L, 2168L, 6402L, 5860L, 5101L, 7164L, 7402L, 6363L, 2102L, 4944L, 5753L, 3301L, 6833L, 4600L, 2144L, 4333L, 6866L, 2225L, 5810L, 2845L, 2223L, 7488L, 7117L, 930L, 3394L, 4322L, 6922L, 3640L, 3395L, 1536L, 4071L, 4277L, 4074L, 2209L, 5847L, 6708L, 3305L, 4234L, 3206L, 6044L, 1553L, 2438L, 1864L, 6094L, 2382L, 6182L, 4357L, 344L, 7505L, 671L, 1906L, 5715L, 2485L, 342L, 3031L, 6337L, 3820L, 5823L, 5329L, 2142L, 5579L, 5928L, 6163L, 1939L, 7343L, 143L, 6335L, 2925L, 7364L, 5618L, 3464L, 3449L, 2934L, 5170L, 3385L, 7444L, 6947L, 1244L, 3186L, 4589L, 5452L, 7059L, 2270L, 5468L, 4072L, 1208L, 4570L, 1450L, 3743L, 5320L, 7231L, 5013L, 5517L, 5737L, 675L, 15L, 5628L, 5180L, 1455L, 1172L, 6438L, 6895L, 1233L, 473L, 2005L, 7532L, 4706L, 4606L, 5645L, 6068L, 6106L, 346L, 6373L, 4609L, 1180L, 6494L, 4293L, 5911L, 940L, 7074L, 4629L, 6717L, 1091L, 2406L, 4056L, 4218L, 4504L, 4920L, 1341L, 4531L, 3878L, 6283L, 4100L, 7077L, 5449L, 3475L, 6147L, 5646L, 13L, 31L, 4964L, 7182L, 6408L, 4139L, 7196L, 7443L, 6460L, 1632L, 5559L, 5385L, 3000L, 6732L, 7125L, 5312L, 6680L, 2015L, 7392L, 7474L, 3884L, 2136L, 7221L, 5149L, 690L, 6681L, 1836L, 7535L, 2188L, 6709L, 7421L, 1067L, 3059L, 3649L, 6710L, 5142L, 7058L, 6131L, 3106L, 5014L, 3973L, 6778L, 5576L, 3660L, 1422L, 7511L, 1689L, 5627L, 6478L, 3446L, 1131L, 576L, 193L, 995L, 2553L, 7132L, 5296L, 2066L, 3669L, 7108L, 6461L, 4725L, 6645L, 2191L, 3874L, 2593L, 515L, 4019L, 6280L, 2380L, 7101L, 4410L, 6975L, 5508L, 3447L, 5098L, 372L, 4041L, 7143L, 7278L, 4850L, 3793L, 6273L, 5227L, 4246L, 2409L, 7002L, 4186L, 953L, 6845L, 3955L, 3288L, 5946L, 1921L, 6626L, 5118L, 856L, 2498L, 4466L, 4726L, 7390L, 1055L, 5595L, 2628L, 5619L, 7456L, 3401L, 4881L, 6456L, 301L, 5451L, 5230L, 7129L, 7106L, 233L, 2510L, 2234L, 7243L, 7199L, 2306L, 7338L, 5407L, 7020L, 3928L, 1301L, 1977L, 5092L, 6931L, 3457L, 2105L, 7024L, 1877L, 5610L, 1749L, 2794L, 7426L, 6153L, 950L, 3967L, 4198L, 2619L, 4485L, 7176L, 570L, 2981L, 5871L, 6471L, 5716L, 87L, 892L, 7230L, 6643L, 3168L, 3917L, 3896L, 3835L, 1540L, 7350L, 2641L, 2169L, 7159L, 6811L, 4193L, 7262L, 5505L, 6440L, 7018L, 5539L, 7418L, 1982L, 3456L, 1403L, 7206L, 4057L, 4713L, 7507L, 2276L, 3925L, 1268L, 5695L, 2640L, 6341L, 1879L, 3226L, 2986L, 5298L, 3668L, 7284L, 3221L, 7053L, 2746L, 2588L, 7093L, 5292L, 5089L, 267L, 1985L, 5722L, 5021L, 6721L, 5097L, 1666L, 5929L, 866L, 4262L, 5927L, 7471L, 4927L, 4618L, 2865L, 403L, 6599L, 436L, 7170L, 6618L, 1502L, 175L, 5551L, 4652L, 6563L, 4540L, 7523L, 6515L, 6856L, 3525L, 4653L, 5177L, 1328L, 1815L, 2917L, 6174L, 3290L, 609L, 1869L, 6007L, 187L, 6020L, 5686L, 1038L, 1022L, 6919L, 7433L, 5046L, 7379L, 3529L, 3223L, 1811L, 2378L, 5421L, 4334L, 3246L, 2590L, 6480L, 6365L, 2821L, 2263L, 6735L, 739L, 2070L, 3650L, 4754L, 3376L, 1519L, 6750L, 4597L, 6067L, 4155L, 1174L, 4435L, 2114L, 4324L, 6372L, 2459L, 3725L, 5015L, 7270L, 4782L, 1858L, 3644L, 1475L, 3337L, 3816L, 6087L, 5398L, 7218L, 5252L, 6820L, 4946L, 2663L, 6099L, 4965L, 6157L, 7451L, 3998L, 6702L, 4310L, 4279L, 297L, 5850L, 5131L, 3008L, 3225L, 5751L, 1059L, 745L, 3436L, 7304L, 702L, 795L, 2250L, 5201L, 6346L, 6188L, 7110L, 6749L, 7089L, 5060L, 5164L, 508L, 6991L, 3396L, 4421L, 5382L, 4942L, 6221L);
  private final Set<Long> GN_PLUS_TEN = Sets.newHashSet(2908L, 1793L, 6008L, 7095L, 215L, 1751L, 4750L, 5099L, 4766L, 4423L, 3812L, 6472L, 4721L, 596L, 3927L, 6604L, 757L, 453L, 1260L, 7449L, 1579L, 5402L, 5573L, 2531L, 6293L, 3659L, 4972L, 6425L, 2736L, 4723L, 2312L, 6592L, 4439L, 5983L, 7168L, 4867L, 5676L, 3498L, 6624L, 6656L, 4229L, 3407L, 6568L, 5145L, 3562L, 4470L, 7105L, 5165L, 393L, 6674L, 2041L, 5047L, 3489L, 6082L, 5760L, 3141L, 7528L, 4034L, 4935L, 6493L, 4433L, 6443L, 7380L, 6557L, 7531L, 1847L, 6507L, 2496L, 2374L, 3417L, 2721L, 4368L, 3831L, 5396L, 1612L);
  private final Set<Long> GN_FINAL = Sets.newHashSet(5654L, 2386L, 7087L, 1510L, 2923L, 5381L, 1873L, 7096L, 269L, 7386L, 1289L, 3833L, 5631L, 6985L, 6304L, 3499L, 3129L, 1185L, 1147L, 3153L, 5704L, 1525L, 503L, 5854L, 7186L, 2599L, 7274L, 791L, 2933L, 1791L, 6831L, 4686L, 171L, 5519L, 4112L, 5270L, 1442L, 6635L, 1154L, 959L, 5325L, 729L, 6768L, 5913L, 5642L, 3915L, 4988L, 4816L, 5634L, 4876L, 237L, 1720L, 6130L, 5933L, 2785L, 1194L, 6564L, 5806L, 6242L, 698L, 5906L, 6150L, 4959L, 2970L, 4170L, 6303L, 3230L, 7477L, 4191L, 6876L, 763L, 1882L, 7285L, 6593L);

  private final Set<Long> DBP_EIGHTY = Sets.newHashSet(281L, 426L, 6559L, 4799L, 2422L, 5613L, 5840L, 6745L, 2471L, 7212L, 4874L, 2371L, 5653L, 3740L, 7097L, 902L, 6281L, 5741L, 636L, 4253L, 43L, 1550L, 5467L, 4865L, 3172L, 428L, 5934L, 6388L, 5762L, 1706L, 5086L, 5910L, 4282L, 6448L, 5744L, 4430L, 6673L, 5019L, 3112L, 5315L, 5277L, 4355L, 5349L, 1946L, 455L, 1577L, 1904L, 1970L, 7131L, 6755L, 2366L, 6598L, 4613L, 1118L, 5800L, 7351L, 7249L, 3583L, 167L, 4000L, 5507L, 4642L, 3371L, 5037L, 6685L, 6894L, 7476L, 5534L, 7331L, 6394L, 6320L, 6339L, 6211L, 2910L, 5693L, 4801L, 4223L, 3581L, 5529L, 6792L, 2744L, 422L, 2575L, 6107L, 4587L, 4906L, 4773L, 755L, 2094L, 6923L, 4049L, 1377L, 1032L, 6644L, 5428L, 3774L, 4259L, 2141L, 2869L, 3964L, 3505L, 5665L, 2008L, 6338L, 3891L, 3714L, 4489L, 2440L, 4519L, 3985L, 7388L, 2625L, 4981L, 5694L, 6078L, 5724L, 5712L, 534L, 2807L, 2669L, 6691L, 4119L, 5278L, 2812L, 3975L, 5231L, 2297L, 1467L, 2294L, 7291L, 6550L, 6500L, 25L, 2771L, 2272L, 978L, 5765L, 896L, 7408L, 5364L, 2715L, 1204L, 6807L, 2307L, 6245L, 2953L, 2047L, 5193L, 4892L, 3680L, 5572L, 1874L, 2030L, 7012L, 449L, 4610L, 7334L, 6993L, 5412L, 7409L, 4366L, 6939L, 6916L, 2345L, 95L, 3167L, 2951L, 4738L, 2475L, 2176L, 4037L, 3905L, 4116L, 4775L, 4066L, 2989L, 2586L, 1388L, 7035L, 5272L, 4573L, 83L, 1524L, 3692L, 5128L, 6615L, 4617L, 4152L, 6065L, 7517L, 1L, 712L, 6540L, 2871L, 2081L, 3207L, 7317L, 4238L, 3518L, 891L, 1767L, 5605L, 5662L, 369L, 824L, 6101L, 5978L, 4689L, 6055L, 7263L, 6941L, 2864L, 4576L, 2227L, 5204L, 3881L, 6527L, 3477L, 3972L, 5804L, 6489L, 7412L, 4936L, 4089L, 3768L, 3004L, 6002L, 2149L, 2060L, 4581L, 1539L, 3753L, 4514L, 3357L, 7299L, 2926L, 7213L, 3708L, 5799L, 3189L, 7271L, 4563L, 4945L, 3234L, 6123L, 3519L, 3775L, 932L, 4294L, 4069L, 311L, 263L, 3789L, 4973L, 415L, 7194L, 6309L, 3418L, 5808L, 1060L, 2361L, 1656L, 4058L, 2274L, 3673L, 6530L, 3427L, 7119L, 1944L, 7246L, 430L, 6437L, 1001L, 2878L, 2123L, 4506L, 6386L, 460L, 4840L, 325L, 3434L, 3252L, 7467L, 5476L, 6963L, 4442L, 1349L, 2243L, 5621L, 1437L, 646L, 1829L, 4814L, 6387L, 1409L, 1907L, 5909L, 3887L, 4043L, 5664L, 5736L, 3313L, 2900L, 4476L, 4839L, 4097L, 3851L, 5371L, 5897L, 2642L, 2815L, 4225L, 2150L, 7226L, 6287L, 3265L, 1746L, 1190L, 7006L, 4542L, 2792L, 3300L, 774L, 5455L, 6648L, 1351L, 3467L, 2860L, 5671L, 3759L, 7011L, 3322L, 4603L, 6499L, 4374L, 5048L, 6298L, 6607L, 6942L, 1083L, 4788L, 2881L, 1411L, 3422L, 3572L, 1487L, 6209L, 2451L, 1535L, 7112L, 3731L, 4956L, 7417L, 5668L, 5848L, 4821L, 2776L, 7083L, 4734L, 1911L, 5080L, 3674L, 1770L, 6590L, 4300L, 6620L, 1150L, 6469L, 5858L, 5181L, 5499L, 3472L, 2761L, 3170L, 4471L, 4393L, 4508L, 3123L, 7269L, 6178L, 5688L, 3868L, 219L, 2885L, 5713L, 4362L, 4440L, 7187L, 716L, 6252L, 4194L, 432L, 7516L, 6775L, 1206L, 5569L, 2101L, 7487L, 3696L, 4021L, 6384L, 1415L, 7502L, 2877L, 5236L, 5365L, 2085L, 5L, 4715L, 704L, 5256L, 5049L, 6237L, 2156L, 3091L, 1122L, 4546L, 5330L, 2048L, 5659L, 6573L, 4007L, 7405L, 5494L, 89L, 5453L, 2921L, 3195L, 5584L, 6914L, 6546L, 1444L, 7069L, 4747L, 2763L, 6391L, 1368L, 3061L, 3070L, 7495L, 4697L, 6549L, 611L, 255L, 6830L, 5125L, 6927L, 4998L, 7255L, 3389L, 389L, 1610L, 6565L, 3415L, 6995L, 5403L, 6730L, 4560L, 241L, 3724L, 3171L, 4303L, 4047L, 6741L, 3485L, 3336L, 1841L, 3959L, 6299L, 4408L, 6751L, 7260L, 3734L, 2966L, 5876L, 2774L, 5937L, 5506L, 3594L, 6354L, 5069L, 854L, 51L, 4790L, 2636L, 4040L, 5566L, 5624L, 405L, 2445L, 5948L, 1343L, 5683L, 85L, 1963L, 7003L, 4907L, 6219L, 2487L, 5301L, 3205L, 2342L, 4449L, 5248L, 4615L, 1644L, 6508L, 2620L, 141L, 4108L, 6195L, 1262L, 1988L, 1512L, 3824L, 2850L, 6688L, 5215L, 5564L, 7056L, 2164L, 2177L, 3461L, 6419L, 6167L, 491L, 3671L, 5083L, 6466L, 761L, 4084L, 6776L, 2968L, 1984L, 1258L, 3511L, 6046L, 3542L, 5387L, 1771L, 3442L, 109L, 5339L, 6168L, 5970L, 4925L, 275L, 4210L, 2915L, 1678L, 5759L, 5316L, 6957L, 445L, 5726L, 5191L, 1993L, 3805L, 1291L, 624L, 1965L, 2173L, 7073L, 4276L, 4632L, 3739L, 6748L, 2857L, 6240L, 983L, 6112L, 6327L, 6819L, 5546L, 5141L, 4953L, 6395L, 4012L, 7248L, 2710L, 1276L, 2299L, 4656L, 6088L, 441L, 2134L, 6682L, 4350L, 3902L, 6485L, 3229L, 3282L, 4273L, 5958L, 2367L, 7381L, 574L, 5055L, 39L, 149L, 4068L, 3484L, 6551L, 4255L, 1053L, 5831L, 5433L, 6912L, 6282L, 6061L, 6127L, 3334L, 7233L, 3532L, 1521L, 1218L, 4344L, 5841L, 289L, 2587L);
  private final Set<Long> DBP_PLUS_TEN = Sets.newHashSet(3379L, 6132L, 6095L, 6014L, 5091L, 4612L, 6917L, 4337L, 6782L, 1596L, 5034L, 2080L, 4951L, 2198L, 7241L, 6162L, 961L, 1340L, 4101L, 724L, 7296L, 797L, 4937L, 7060L, 1918L, 7510L, 2257L, 4315L, 3006L, 6026L, 4291L, 4228L, 7483L, 7070L, 4358L, 5184L, 3381L, 5697L, 4487L, 3781L, 71L, 7098L, 3843L, 2288L, 323L, 493L, 2480L, 926L, 6943L, 938L, 2135L, 6220L, 6504L, 2985L, 7348L, 3103L, 747L, 6075L, 5130L, 5939L, 2759L, 4536L, 5085L, 4091L, 580L, 3365L, 374L, 2261L, 1120L, 3164L, 6144L, 5362L, 5719L, 1531L, 7457L, 4373L, 5901L);
  private final Set<Long> DBP_FINAL = Sets.newHashSet(7478L, 6862L, 2443L, 1915L, 5981L, 5233L, 7148L, 6892L, 5604L, 4882L, 6328L, 7398L, 6362L, 1649L, 6539L, 6920L, 6878L, 3736L, 4624L, 7375L, 2678L, 5883L, 6177L, 3383L, 1856L, 6913L, 6936L, 2762L, 7365L, 1116L, 2322L, 131L, 7153L, 2686L, 5435L, 6930L, 7247L, 5446L, 5547L, 3555L, 7395L, 1852L, 4868L, 2835L, 6156L, 2331L, 3628L, 7166L, 5733L, 3806L, 7384L, 3652L, 6699L, 5344L, 5813L, 5269L, 4781L, 731L, 946L, 5692L, 5447L, 3539L, 5158L, 7504L, 5198L, 4528L, 6079L, 1887L, 2253L, 6104L, 6857L, 5422L, 2646L, 4710L, 6965L, 7217L, 1668L);

  private final Set<Long> NYT_EIGHTY = Sets.newHashSet(122L, 2315L, 3541L, 1189L, 1321L, 6911L, 4414L, 3538L, 2585L, 697L, 1086L, 1609L, 2267L, 1374L, 4722L, 2104L, 4299L, 56L, 4356L, 42L, 4221L, 1872L, 1910L, 1052L, 3531L, 1792L, 2011L, 4118L, 2942L, 1207L, 4543L, 982L, 676L, 2046L, 2L, 1043L, 2907L, 4190L, 3069L, 1814L, 5002L, 4432L, 2909L, 2675L, 180L, 4753L, 5011L, 1631L, 3950L, 613L, 3517L, 4110L, 3245L, 3675L, 2093L, 38L, 756L, 2128L, 931L, 3476L, 6274L, 3802L, 2298L, 1226L, 4919L, 394L, 2014L, 82L, 3312L, 4090L, 3204L, 3707L, 3819L, 232L, 345L, 1962L, 1365L, 1171L, 6864L, 1285L, 3267L, 575L, 2143L, 5028L, 240L, 1654L, 3742L, 1119L, 3264L, 3493L, 341L, 435L, 1261L, 1886L, 5568L, 5384L, 1810L, 2079L, 3076L, 2262L, 6622L, 461L, 2065L, 2383L, 2439L, 5445L, 1671L, 2226L, 3113L, 1922L, 492L, 5620L, 202L, 264L, 3780L, 1327L, 3528L, 925L, 218L, 1449L, 170L, 5372L, 3744L, 4013L, 2897L, 78L, 1335L, 719L, 1203L, 3691L, 1298L, 284L, 212L, 94L, 4866L, 3637L, 1062L, 837L, 2296L, 3368L, 2326L, 1916L, 3023L, 2639L, 3784L, 1876L, 404L, 1881L, 2256L, 929L, 3374L, 2187L, 1773L, 790L, 1474L, 256L, 723L, 3104L, 994L, 3240L, 262L, 2328L, 4018L, 811L, 156L, 1367L, 84L, 2391L, 2313L, 901L, 2497L, 1414L, 2863L, 949L, 3074L, 1652L, 150L, 762L, 511L, 5853L, 1158L, 2478L, 727L, 1186L, 2743L, 3421L, 2293L, 2753L, 347L, 3291L, 674L, 4365L, 831L, 1115L, 2458L, 32L, 623L, 4326L, 5276L, 144L, 4549L, 1267L, 2987L, 3416L, 715L, 754L, 5214L, 3044L, 2155L, 3335L, 579L, 3748L, 784L, 4150L, 1184L, 1633L, 4192L, 2582L, 4073L, 2702L, 174L, 4691L, 1920L, 12L, 1410L, 448L, 2665L, 3159L, 3152L, 192L, 1290L, 4572L, 50L, 4997L, 5931L, 1348L, 3408L, 2592L, 2242L, 573L, 1518L, 617L, 3323L, 1750L, 2735L, 3448L, 4480L, 343L, 3713L, 142L, 2884L, 30L, 730L, 2040L, 1217L, 1790L, 1997L, 3122L, 4512L, 236L, 701L, 3150L, 3028L, 1695L, 2140L, 4461L, 1376L, 300L, 2589L, 1533L, 5305L, 1117L, 1275L, 2530L, 148L, 890L, 5485L, 3924L, 260L, 3111L, 4292L, 2870L, 2381L, 5146L, 4115L, 2880L, 5018L, 3994L, 4545L, 1992L, 1451L, 5221L, 1443L, 536L, 2760L, 1037L, 2756L, 1013L, 752L, 5938L, 571L, 472L, 1300L, 1269L, 2091L, 310L, 1945L, 1835L, 2360L, 871L, 4309L, 533L, 58L, 1441L, 2311L, 5192L, 3463L, 3482L, 1903L, 1470L, 3030L, 3658L, 4469L, 1625L, 2163L, 825L, 773L, 2822L, 1149L, 2193L, 274L, 1259L, 1629L, 2935L, 6839L, 6054L, 2967L, 2486L, 2782L, 3978L, 3787L, 4482L, 599L, 3400L, 1846L, 2344L, 1193L, 2147L, 4166L, 1004L, 3815L, 2784L, 2117L, 1837L, 4422L, 794L, 3898L, 1412L, 1905L, 3733L, 416L, 2645L, 1797L, 2405L, 2108L, 5190L, 1870L, 14L, 5951L, 1552L, 1090L, 288L, 905L, 504L, 2029L, 637L, 627L, 1935L, 977L, 5721L, 3556L, 3060L, 1679L, 3179L, 3032L, 6187L, 652L, 7082L, 2175L, 1228L, 1727L, 5378L, 565L, 140L, 1543L, 960L, 214L, 2448L, 3735L, 4862L, 1429L, 2437L, 839L, 516L, 5448L, 3445L, 3304L, 2844L, 1719L, 1031L, 3188L, 3984L, 70L, 2474L, 2905L, 3929L, 1294L, 6589L, 1688L, 958L, 452L, 4050L, 4154L, 2656L, 427L, 2564L, 402L, 610L, 2691L, 895L, 3346L, 3460L, 5070L, 5255L, 411L, 687L, 5171L, 3378L, 952L, 4455L, 1146L, 2876L, 1567L, 1509L, 1516L, 324L, 4011L, 4963L, 1436L, 2795L, 6L, 3544L, 6498L, 2887L, 2133L, 2685L, 2810L, 4302L, 4614L, 3616L, 685L, 2454L, 2891L, 176L, 252L, 3723L, 635L, 4062L, 4070L, 2770L, 939L, 1454L, 423L, 444L, 1665L, 5183L, 2167L, 619L, 4335L, 2224L, 373L, 1319L, 4926L, 4805L, 3005L, 4486L, 1359L, 2899L, 3584L, 3166L, 2004L, 1478L, 2574L, 1205L, 502L, 1721L, 569L, 1197L, 4416L, 1851L, 2138L, 1391L, 1520L, 5714L, 108L, 1816L, 1066L, 3299L, 853L, 4438L, 823L, 1468L, 86L, 2269L, 4367L, 357L, 4290L, 1058L, 1313L, 4696L, 1393L, 3679L, 1215L, 1121L, 645L, 857L, 1914L, 166L, 282L, 1182L, 742L, 656L, 2673L, 3118L, 1595L, 3287L, 2275L, 544L, 3561L, 3880L, 1969L, 1342L, 3256L, 3410L, 248L, 4608L, 865L, 2706L, 80L, 24L, 1964L, 3501L, 425L, 3136L, 4768L, 689L, 1503L, 1265L, 3890L, 1859L, 268L, 2999L, 1707L, 1501L, 4584L, 873L, 1156L, 3471L, 4316L, 2952L, 1173L, 2775L, 454L, 6301L, 1056L, 2598L, 4567L, 3524L, 1017L, 595L, 2519L, 4L, 1021L, 216L, 3443L, 3738L, 3864L, 1983L, 3587L, 1220L, 4107L, 431L, 3752L, 484L, 796L, 2495L, 5203L, 738L, 5857L, 1224L, 711L, 3873L, 2789L, 2450L, 4616L);
  private final Set<Long> NYT_PLUS_TEN = Sets.newHashSet(3384L, 270L, 3689L, 6642L, 200L, 4611L, 5059L, 5483L, 3003L, 3976L, 5538L, 5124L, 2122L, 88L, 5481L, 4605L, 4224L, 1725L, 2110L, 1054L, 429L, 186L, 490L, 198L, 744L, 4254L, 2283L, 4227L, 3308L, 5096L, 6538L, 1466L, 2273L, 1588L, 2305L, 4141L, 4955L, 2442L, 937L, 5169L, 1387L, 3804L, 459L, 5416L, 4258L, 1232L, 3224L, 1432L, 2365L, 4138L, 4883L, 4006L, 2389L, 2552L, 52L, 3163L, 3665L, 3582L, 3764L, 4033L, 4237L, 2421L, 1179L, 855L, 298L, 682L, 266L, 1195L, 2372L, 62L, 421L, 2100L, 703L, 1130L, 986L);
  private final Set<Long> NYT_FINAL = Sets.newHashSet(2260L, 5836L, 4048L, 322L, 2432L, 1857L, 4312L, 5432L, 1257L, 1828L, 2720L, 3627L, 2916L, 2484L, 3996L, 3651L, 4448L, 2617L, 1408L, 5591L, 3758L, 670L, 296L, 1039L, 272L, 1578L, 1523L, 2668L, 4714L, 647L, 1987L, 1002L, 5157L, 2340L, 2635L, 110L, 3611L, 3974L, 368L, 392L, 4562L, 1486L, 5533L, 3823L, 4083L, 4232L, 1855L, 1981L, 4099L, 2596L, 1729L, 1690L, 2780L, 419L, 4217L, 2324L, 5175L, 1840L, 5241L, 3672L, 3182L, 3233L, 0L, 130L, 280L, 4288L, 1878L, 3643L, 100L, 2758L, 2709L, 758L, 973L, 6545L, 5311L);

  private final Set<Long> FB_EIGHTY = Sets.newHashSet(145L, 3369L, 57L, 4884L, 7115L, 7288L, 4646L, 4834L, 512L, 6029L, 4757L, 1005L, 5030L, 5143L, 2316L, 257L, 7204L, 3404L, 2674L, 7039L, 6015L, 6244L, 7222L, 5797L, 2936L, 3720L, 3769L, 2249L, 2849L, 6250L, 3165L, 6795L, 3412L, 5293L, 1221L, 6689L, 5217L, 5896L, 2823L, 759L, 5162L, 1433L, 3591L, 4700L, 4561L, 5641L, 1430L, 7102L, 826L, 4148L, 5796L, 4886L, 2063L, 3811L, 858L, 1544L, 566L, 5739L, 5032L, 1726L, 6521L, 5071L, 505L, 7030L, 537L, 6761L, 1947L, 4987L, 5439L, 2745L, 3862L, 5331L, 6698L, 5837L, 2783L, 3588L, 7332L, 3938L, 3861L, 4151L, 4627L, 6210L, 6712L, 1900L, 33L, 4933L, 6869L, 5942L, 6843L, 1227L, 79L, 7061L, 3257L, 4681L, 3155L, 517L, 6164L, 7281L, 1018L, 6870L, 249L, 6907L, 5310L, 832L, 1517L, 5100L, 2436L, 2392L, 4270L, 6798L, 4434L, 5242L, 5075L, 2670L, 5375L, 4124L, 5279L, 3066L, 3117L, 4063L, 6860L, 5427L, 4885L, 3428L, 2569L, 5805L, 7257L, 1936L, 7211L, 638L, 2565L, 6902L, 6571L, 6921L, 3930L, 1104L, 5176L, 5699L, 6517L, 3190L, 6270L, 7076L, 3865L, 1178L, 6553L, 1371L, 4638L, 4791L, 3049L, 5194L, 6882L, 5399L, 4740L, 5240L, 3411L, 7046L, 6316L, 7366L, 3402L, 5200L, 7415L, 2781L, 653L, 6229L, 1183L, 177L, 2754L, 1547L, 3856L, 2943L, 3151L, 2911L, 3726L, 4647L, 4702L, 6655L, 5821L, 6184L, 2449L, 7047L, 3638L, 1534L, 5065L, 5924L, 2791L, 4544L, 3050L, 3937L, 5003L, 6677L, 4787L, 59L, 1630L, 4451L, 7453L, 6353L, 5701L, 7455L, 1934L, 5515L, 6652L, 7512L, 5090L, 3989L, 2597L, 53L, 5419L, 7319L, 7198L, 420L, 3676L, 7123L, 4481L, 5554L, 6814L, 2583L, 686L, 3024L, 1894L, 4342L, 4372L, 1762L, 1413L, 4897L, 2820L, 6450L, 157L, 1003L, 5575L, 1863L, 7465L, 4142L, 3502L, 5127L, 5386L, 5346L, 648L, 4861L, 1708L, 3951L, 5172L, 4289L, 5352L, 4858L, 4311L, 6052L, 5577L, 4376L, 3617L, 6433L, 485L, 5991L, 5076L, 1611L, 4941L, 6997L, 5355L, 3598L, 2129L, 1653L, 628L, 7472L, 572L, 4336L, 6042L, 6179L, 3866L, 7122L, 1386L, 3785L, 5380L, 6336L, 5126L, 481L, 5957L, 6886L, 2811L, 1871L, 5436L, 3790L, 4667L, 7520L, 6977L, 3183L, 3002L, 6011L, 3590L, 3077L, 1270L, 3875L, 6492L, 6286L, 1634L, 3292L, 4764L, 5611L, 2494L, 6050L, 7356L, 6025L, 913L, 6114L, 123L, 6889L, 6990L, 2920L, 4692L, 3274L, 2325L, 1897L, 4847L, 1375L, 677L, 1959L, 6012L, 838L, 4639L, 2924L, 3241L, 5912L, 1252L, 1722L, 5031L, 7436L, 4637L, 3439L, 3380L, 7163L, 6043L, 4880L, 4662L, 1320L, 273L, 6097L, 253L, 4209L, 111L, 6290L, 1336L, 5426L, 2222L, 2511L, 5959L, 1096L, 5259L, 199L, 2690L, 1187L, 6915L, 6627L, 6728L, 7267L, 5682L, 299L, 5602L, 5588L, 3351L, 2109L, 5409L, 2581L, 874L, 5244L, 6641L, 5790L, 7197L, 4453L, 1044L, 6903L, 5835L, 7028L, 2657L, 3268L, 5766L, 6937L, 3L, 728L, 6906L, 5133L, 7316L, 2341L, 4513L, 6805L, 4492L, 3977L, 7424L, 7277L, 1360L, 1626L, 5294L, 2898L, 5229L, 4797L, 5710L, 614L, 7071L, 1087L, 5051L, 4233L, 5859L, 6746L, 6284L, 3324L, 6484L, 5266L, 4327L, 3836L, 2424L, 6059L, 2492L, 2314L, 2520L, 6173L, 1730L, 1229L, 5058L, 1917L, 7057L, 4462L, 4601L, 3382L, 5882L, 2072L, 5637L, 6488L, 2174L, 7508L, 5484L, 7174L, 7010L, 5798L, 1832L, 6835L, 4960L, 7341L, 7048L, 3386L, 5462L, 4443L, 6970L, 2433L, 4046L, 4711L, 7052L, 6119L, 221L, 2888L, 7440L, 1063L, 3007L, 4051L, 2886L, 3513L, 2479L, 2790L, 1691L, 2969L, 4952L, 3114L, 3903L, 6348L, 7081L, 3850L, 4857L, 6305L, 2455L, 424L, 4285L, 2906L, 6619L, 3979L, 5636L, 7342L, 5168L, 4595L, 1075L, 1500L, 4550L, 1757L, 6285L, 2460L, 7109L, 6073L, 3029L, 1225L, 1469L, 743L, 5062L, 5406L, 2045L, 2892L, 4628L, 6266L, 3710L, 7120L, 5526L, 271L, 6754L, 395L, 6137L, 3702L, 4263L, 7527L, 1615L, 4022L, 5826L, 5042L, 2796L, 3565L, 5219L, 1366L, 2423L, 3375L, 6317L, 4649L, 3557L, 6058L, 4991L, 2031L, 2513L, 6547L, 1240L, 6726L, 3119L, 4483L, 3751L, 101L, 6981L, 2961L, 7369L, 6687L, 7459L, 5147L, 4231L, 6859L, 2676L, 7519L, 7043L, 1394L, 4117L, 5793L, 4674L, 6256L, 7068L, 7016L, 6989L, 5383L, 6929L, 7268L, 2139L, 7501L, 7195L, 5068L, 4716L, 4751L, 4111L, 6938L, 6380L, 7266L, 7025L, 4502L, 7308L, 6191L, 2115L, 3995L, 2334L, 7072L, 6225L, 6192L, 3540L, 5945L, 872L, 3560L, 3444L, 5829L, 6323L, 1728L, 1479L, 4827L, 840L, 5487L, 3788L, 3612L, 326L, 1998L, 3347L, 7261L, 2183L, 5492L, 4818L, 1057L, 3745L, 5306L, 4520L, 1286L, 3918L, 4704L, 348L, 5212L, 4980L, 5284L, 4800L, 4222L, 4659L, 4695L, 6389L, 3289L, 4236L, 7175L, 6794L, 4962L, 4475L, 6781L, 3749L, 5567L, 2384L, 6260L, 7315L, 3014L, 7515L, 6300L, 5884L, 6193L);
  private final Set<Long> FB_PLUS_TEN = Sets.newHashSet(6881L, 4109L, 5932L, 5379L, 3585L, 2284L, 5073L, 4388L, 683L, 3718L, 2323L, 4769L, 4569L, 2148L, 5992L, 4568L, 6217L, 6762L, 3255L, 3180L, 6275L, 5811L, 6134L, 4128L, 283L, 6049L, 6675L, 4804L, 6355L, 4121L, 5999L, 6800L, 2132L, 5993L, 5770L, 5206L, 5833L, 6623L, 2373L, 1337L, 5649L, 6077L, 2343L, 3698L, 1681L, 1157L, 4806L, 1318L, 4585L, 2194L, 6444L, 6467L, 7454L, 2666L, 1648L, 201L, 6412L, 2939L, 4457L, 3045L, 6806L, 5185L, 4914L, 600L, 2390L, 3760L, 7220L, 3847L, 5652L, 6974L, 6302L, 6253L, 5477L, 5973L, 5758L, 5889L, 3586L);
  private final Set<Long> FB_FINAL = Sets.newHashSet(7239L, 1504L, 3317L, 688L, 5029L, 5136L, 5807L, 4863L, 5224L, 6766L, 5592L, 7485L, 5369L, 6092L, 3690L, 1014L, 5103L, 1646L, 4511L, 5417L, 2329L, 261L, 3393L, 4272L, 1452L, 3717L, 5720L, 4456L, 7314L, 618L, 7227L, 3285L, 2268L, 6232L, 2327L, 7448L, 7252L, 4853L, 5482L, 4402L, 6053L, 3965L, 6511L, 6875L, 7509L, 3358L, 2170L, 3919L, 5012L, 6625L, 5074L, 5182L, 1798L, 5745L, 7320L, 620L, 63L, 4167L, 4529L, 7371L, 3548L, 753L, 1589L, 2473L, 2285L, 5556L, 375L, 3160L, 974L, 657L, 203L, 4415L, 1642L, 4707L, 2707L, 4535L, 265L);

  @Test
  public void sourceSelectCountTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices();

    DataSet<Vertex<Long, ObjectMap>> gn = vertices
        .filter(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> nyt = vertices
        .filter(new SourceFilterFunction(Constants.NYT_NS));

    assertEquals(749L, gn.count());
    assertEquals(755, nyt.count());
  }

  /**
   * Pre-test for createReprTest, 1x all elements one source, 1x mixed
   */
  @Test
  public void manyOneSourceCreateTripletsTest() throws Exception {
    // one source
    DataSet<MergeGeoTriplet> sai = getGnNytVertices()
        .filter(vertex -> vertex.getValue().getBlockingKey().equals("sai"))
        .map(new MergeGeoTupleCreator())
        .groupBy(7) // tuple blocking key
        .reduceGroup(new MergeGeoTripletCreator(2, Constants.NYT_NS, true))
        .map(triplet -> {
          Assert.assertEquals(triplet.getSrcId(), triplet.getTrgId());
          return triplet;
        });
    Assert.assertEquals(5, sai.count());

    // mixed sources
    DataSet<MergeGeoTriplet> cha = getGnNytVertices()
        .filter(vertex -> vertex.getValue().getBlockingKey().equals("cha"))
        .map(new MergeGeoTupleCreator())
        .groupBy(7) // tuple blocking key
        .reduceGroup(new MergeGeoTripletCreator(2, Constants.NYT_NS, true))
        .map(triplet -> {
//          LOG.info(triplet.toString());
          Assert.assertNotEquals(triplet.getSrcId(), triplet.getTrgId());
          return triplet;
        });
    Assert.assertEquals(25, cha.count());
  }

  /**
   * Get all vertices from gn and nyt.
   */
  private DataSet<Vertex<Long, ObjectMap>> getGnNytVertices() {
    DataSet<Vertex<Long, ObjectMap>> reps = getInputGeoGraph();

    DataSet<Vertex<Long, ObjectMap>> first = reps
        .filter(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> second = reps
        .filter(new SourceFilterFunction(Constants.NYT_NS));

    return first.union(second);
  }

  /**
   * Get all vertices from geo graph with basic representatives.
   */
  private DataSet<Vertex<Long, ObjectMap>> getInputGeoGraph() {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, ObjectMap> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph();

    return graph
        .mapVertices(new InternalTypeMapFunction())
        .getVertices()
        .map(new AddShadingTypeMapFunction())
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));
  }

  @Test
  public void createReprTest() throws Exception {
    DataSet<MergeGeoTriplet> result = getGnNytVertices()
        .runOperation(new CandidateCreator(
            BlockingStrategy.STANDARD_BLOCKING,
            DataDomain.GEOGRAPHY,
            Constants.COSINE_TRIGRAM,
            Constants.NYT_NS,
            2,
            env));
//        .map(new MergeGeoTupleCreator()) // 1504 correct

    DataSet<MergeGeoTriplet> singleEntities = result.join(result)
        .where(0)
        .equalTo(1)
        .with((left, right) -> left)
        .returns(new TypeHint<MergeGeoTriplet>() {
        })
        .distinct(0, 1);
    LOG.info(result.count());
    LOG.info(singleEntities.count());
//    Assert.assertEquals(771, result.count());
//    Assert.assertEquals(60, singleEntities.count());

    getGnNytVertices().leftOuterJoin(result)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>())
        .leftOuterJoin(result)
        .where(0)
        .equalTo(1)
        .with(new LeftMinusRightSideJoinFunction<>())
        .print();
  }

  @Test
  public void lshSingleSourceVariationTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    final Graph<Long, ObjectMap, NullValue> baseGraph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class)
            .mapVertices(new InternalTypeMapFunction());

    DataSet<Vertex<Long, ObjectMap>> nytSource = baseGraph.getVertices()
        .filter(new SourceFilterFunction(Constants.NYT_NS));
    DataSet<Vertex<Long, ObjectMap>> dbpSource = baseGraph.getVertices()
        .filter(new SourceFilterFunction(Constants.DBP_NS));
    DataSet<Vertex<Long, ObjectMap>> fbSource = baseGraph.getVertices()
        .filter(new SourceFilterFunction(Constants.FB_NS));

    IncrementalClustering nytClustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.SINGLE_SETTING)
        .setMatchElements(nytSource)
        .setNewSource(Constants.NYT_NS)
        .setDataSources(Lists.newArrayList(Constants.GN_NS, Constants.NYT_NS))
        .setBlockingStrategy(BlockingStrategy.LSH_BLOCKING)
        .build();

    Graph<Long, ObjectMap, NullValue> workGraph = baseGraph
        .filterOnVertices(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> clusters = workGraph.run(nytClustering);

    new JSONDataSink(graphPath.concat("/gn-nyt/"), "test")
        .writeVertices(clusters);
    env.execute();

    // second step, merge dbp
    workGraph = new JSONDataSource(
        graphPath.concat("/gn-nyt/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class)
        .mapVertices(new InternalTypeMapFunction());

    IncrementalClustering dbpClustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.SINGLE_SETTING)
        .setMatchElements(dbpSource)
        .setNewSource(Constants.DBP_NS)
        .setDataSources(Lists.newArrayList(Constants.GN_NS, Constants.NYT_NS, Constants.DBP_NS))
        .setBlockingStrategy(BlockingStrategy.LSH_BLOCKING)
        .build();

    clusters = workGraph.run(dbpClustering);

    new JSONDataSink(graphPath.concat("/gn-nyt-dbp/"), "test")
        .writeVertices(clusters);
    env.execute();

    // third step, merge fb
    workGraph = new JSONDataSource(
        graphPath.concat("/gn-nyt-dbp/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class)
        .mapVertices(new InternalTypeMapFunction());

    IncrementalClustering fbClustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.SINGLE_SETTING)
        .setMatchElements(fbSource)
        .setNewSource(Constants.FB_NS)
        .setDataSources(Lists.newArrayList(Constants.GN_NS, Constants.NYT_NS, Constants.DBP_NS, Constants.FB_NS))
        .setBlockingStrategy(BlockingStrategy.LSH_BLOCKING)
        .build();

    clusters = workGraph.run(fbClustering);

    new JSONDataSink(graphPath.concat("/gn-nyt-dbp-fb/"), "test")
        .writeVertices(clusters);
    env.execute();

    clusters.print();
    LOG.info(clusters.count());
    assertEquals(1,2);
//    LOG.info(graph.getVertices().count());
//    LOG.info(graph.getEdgeIds().count());

//    assertEquals(749L, baseGraph.getVertices().count());

  }

  @Test
  public void lshIdfOptBlockingTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

//    graph = graph.filterOnVertices(x ->
//        x.getId() == 2479L || x.getId() == 2478L || x.getId() == 3640L);

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.FIXED_SEQUENCE)
        .setBlockingStrategy(BlockingStrategy.LSH_BLOCKING)
        .build();

    DataSet<Vertex<Long, ObjectMap>> clusters = graph
        .run(clustering);

    // check that all vertices are contained
    DataSet<Tuple2<Long, Long>> singleClusteredVertices = clusters
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>>() {
      @Override
      public void flatMap(Vertex<Long, ObjectMap> cluster, Collector<Tuple2<Long, Long>> out) throws Exception {
        for (Long vertex : cluster.getValue().getVerticesList()) {
          out.collect(new Tuple2<>(cluster.getId(), vertex));
        }
      }
    })
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    DataSet<Vertex<Long, ObjectMap>> with = graph.getVertices()
        .leftOuterJoin(singleClusteredVertices)
        .where(0)
        .equalTo(1)
        .with(new LeftMinusRightSideJoinFunction<>());

    with.print(); // TODO still missing vertices
//    LOG.info("too much: " + with.count());
//
//    LOG.info("vertices in final Clusters: " + singleClusteredVertices.count()); // 3074
//    LOG.info("distinct: " + singleClusteredVertices.distinct(1).count()); // 3054
//    LOG.info("orig verts: " + graph.getVertices().count()); //3054
//    new JSONDataSink(graphPath.concat("/output-lsh-opt-blocking/"), "test")
//        .writeVertices(clusteredVertices);
//    env.execute();
//

//    clusters.filter(cluster -> cluster.getValue().getVerticesList().contains(742L)
//        || cluster.getValue().getVerticesList().contains(3644L)
//        || cluster.getValue().getVerticesList().contains(2345L))
//        .print();

    // check that no vertex contained in the clustered vertices is duplicated
    // TODO duplicates seem to be ok, all checked duplicates overlap and should be in one cluster
    // TODO still need to check why they are not in one cluster
    // TODO LSH candidates seem to be bugged!?
    DataSet<Tuple3<Long, Long, Integer>> sum = clusters
        .map(cluster -> {
//          if (cluster.getValue().getVerticesList().contains(6730L)
//              || cluster.getValue().getVerticesList().contains(2142L)
//              || cluster.getValue().getVerticesList().contains(5499L)) {
//            LOG.info("final out: " + cluster.toString());
//          }
//        if (cluster.getValue().getVerticesList().contains(298L)
//              || cluster.getValue().getVerticesList().contains(299L)
//              || cluster.getValue().getVerticesList().contains(5013L)
//              || cluster.getValue().getVerticesList().contains(5447L)) {
//            LOG.info("final out: " + cluster.toString());
//          }
          return cluster;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, Tuple3<Long, Long, Integer>>() {
          @Override
          public void flatMap(Vertex<Long, ObjectMap> value, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            for (Long aLong : value.getValue().getVerticesList()) {
              out.collect(new Tuple3<>(value.getId(), aLong, 1));
            }
          }
        })
        .groupBy(1)
        .reduceGroup(new GroupReduceFunction<Tuple3<Long,Long,Integer>, Tuple3<Long, Long, Integer>>() {
          @Override
          public void reduce(Iterable<Tuple3<Long, Long, Integer>> values, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            Tuple3<Long, Long, Integer> result = null;
            for (Tuple3<Long, Long, Integer> value : values) {
              if (result == null) {
                result = value;
              } else {
                LOG.info(value.toString());
                result.f2 += value.f2;
              }
            }
            out.collect(result);

          }
        });
//        .sum(2);

    assertEquals(0, sum.filter(tuple -> tuple.f2 > 1).count());
  }

  @Test
  public void noBlockingTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.FIXED_SEQUENCE)
        .setBlockingStrategy(BlockingStrategy.NO_BLOCKING)
        .build();

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph
        .run(clustering);

//    LOG.info(resultVertices.count());
    new JSONDataSink(graphPath.concat("/output-no-blocking/"), "test")
        .writeVertices(resultVertices);
    resultVertices.print();
  }

  @Test
  public void splitSettingIncrementalClusteringTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

    // 80% of NYT, DBP, GN
    IncrementalClustering eightyClustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.SPLIT_SETTING)
        .setPart("eighty")
        .build();

    GN_EIGHTY.addAll(NYT_EIGHTY);
    GN_EIGHTY.addAll(DBP_EIGHTY);

    DataSet<Vertex<Long, ObjectMap>> eightyResult = graph
        .filterOnVertices(new VertexFilterFunction(GN_EIGHTY))
        .run(eightyClustering)
//        .map(x-> {
//          LOG.info(x.toString());
//          return x;
//        })
//        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        ;

    LOG.info("eightyResult: " + eightyResult.count());

    // add 10% of NYT, DBP, GN
    IncrementalClustering plusTenClustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.SPLIT_SETTING)
        .setPart("plusTen")
        .build();

    GN_PLUS_TEN.addAll(NYT_PLUS_TEN);
    GN_PLUS_TEN.addAll(DBP_PLUS_TEN);

    DataSet<Vertex<Long, ObjectMap>> plusTenCompleteInputVertices = graph
        .filterOnVertices(new VertexFilterFunction(GN_PLUS_TEN))
        .getVertices()
        .union(eightyResult);

    Graph<Long, ObjectMap, NullValue> plusTenInputGraph = Graph
        .fromDataSet(plusTenCompleteInputVertices, graph.getEdges(), env);

    DataSet<Vertex<Long, ObjectMap>> plusTenResult = plusTenInputGraph
        .run(plusTenClustering);

    LOG.info("plusTenResult: " + plusTenResult.count());

//    plusTenResult.print();
    // add FB complete
    IncrementalClustering fbClustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.SPLIT_SETTING)
        .setPart("fb")
        .build();

    DataSet<Vertex<Long, ObjectMap>> fbInputVertices = graph.getVertices()
        .filter(new SourceFilterFunction(Constants.FB_NS))
        .union(plusTenResult);

    Graph<Long, ObjectMap, NullValue> fbInputGraph = Graph
        .fromDataSet(fbInputVertices, graph.getEdges(), env);

    DataSet<Vertex<Long, ObjectMap>> fbResult = fbInputGraph
        .run(fbClustering);

    LOG.info("fbResult: " + fbResult.count());

    // add missing 10% NYT, DBP, GN
    IncrementalClustering finalClustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.SPLIT_SETTING)
        .setPart("final")
        .build();

    GN_FINAL.addAll(NYT_FINAL);
    GN_FINAL.addAll(DBP_FINAL);

    DataSet<Vertex<Long, ObjectMap>> finalInput = graph
        .filterOnVertices(new VertexFilterFunction(GN_FINAL))
        .getVertices()
        .union(fbResult);

    Graph<Long, ObjectMap, NullValue> finalInputGraph = Graph
        .fromDataSet(finalInput, graph.getEdges(), env);

    DataSet<Vertex<Long, ObjectMap>> finalResult = finalInputGraph
        .run(finalClustering);

    new JSONDataSink(graphPath.concat("/output-split/"), "test")
        .writeVertices(finalResult);
    LOG.info("finalResult: " + finalResult.count());

//    List<Vertex<Long, ObjectMap>> clusters = resultVertices
//        .collect();
  }
  /**
   * Strategy fixed incremental data sources + check for no duplicates
   */
  @Test
  public void fixedIncClustImplTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.FIXED_SEQUENCE)
        .build();

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph
        .run(clustering);
//    new JSONDataSink(graphPath.concat("/output-reverse/"), "test")
//        .writeVertices(resultVertices);

    List<Vertex<Long, ObjectMap>> clusters = resultVertices
        .collect();

    HashMap<Long, Integer> checkMap = Maps.newHashMap();
    for (Vertex<Long, ObjectMap> vertex : clusters) {
      for (Long single : vertex.getValue().getVerticesList()) {
        if (checkMap.containsKey(single)) {
          assertFalse(true);
//          checkMap.put(single, checkMap.get(single) + 1);
        } else {
          checkMap.put(single, 1);
        }
      }
    }

//    vertices.print();
    Assert.assertEquals(788, clusters.size());
    // check for test if duplicates are available
//    Set<Long> duplicateVertexId = Sets.newHashSet();
//    for (Map.Entry<Long, Integer> entry : checkMap.entrySet()) {
//      if (entry.getValue() > 1) {
//        duplicateVertexId.add(entry.getKey());
//      }
//    }
//
//    HashMap<Long, Vertex<Long, ObjectMap>> duplicateVertex = Maps.newHashMap();
//    for (Vertex<Long, ObjectMap> vertex : graph.getVertices().collect()) {
//      if (duplicateVertexId.contains(vertex.getId())) {
//        duplicateVertex.put(vertex.getId(), vertex);
//      }
//    }
//
//    for (Long aLong : duplicateVertexId) {
//      LOG.info("\n vertex in 2+ clusters: " + aLong);
//      LOG.info("Vertex: " + duplicateVertex.get(aLong).toString());
//      for (Vertex<Long, ObjectMap> cluster : clusters) {
//        if (cluster.getValue().getVerticesList().contains(aLong)) {
//          LOG.info("Contained in cluster: " + cluster.toString());
//        }
//      }
//    }
  }

  /**
   * Used for optimization.
   */
  // TODO check sim comp strat
  // TODO no information is added/removed in merge,
  // TODO last join with FinalMergeGeoVertexCreator unneeded?
  // TODO RepresentativeCreator only adds blocking label, remove and use map function
  @Test
  public void multiSourceTest() throws Exception {
    DataSet<Vertex<Long, ObjectMap>> baseClusters = getGnNytVertices();

    DataSet<Vertex<Long, ObjectMap>> tmp = baseClusters
        .runOperation(new CandidateCreator(
            BlockingStrategy.STANDARD_BLOCKING,
            DataDomain.GEOGRAPHY,
            Constants.COSINE_TRIGRAM,
            Constants.NYT_NS,
            2, env))
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .runOperation(new RepresentativeCreator(
        DataDomain.GEOGRAPHY,
        BlockingStrategy.STANDARD_BLOCKING));

    DataSet<Vertex<Long, ObjectMap>> reps = getInputGeoGraph();

    DataSet<Vertex<Long, ObjectMap>> plusDbp = tmp
        .union(reps.filter(new SourceFilterFunction(Constants.DBP_NS)))
//        .filter(vertex -> vertex.getValue().getBlockingKey().equals("ber"))
        .runOperation(new CandidateCreator(
            BlockingStrategy.STANDARD_BLOCKING,
            DataDomain.GEOGRAPHY,
            Constants.COSINE_TRIGRAM,
            Constants.DBP_NS,
            3, env))
        .flatMap(new DualMergeGeographyMapper(false))
        .map(x-> {
          if (x.getBlockingLabel().equals("ber"))
            LOG.info("FIRST: " + x.toString());
          return x;
        })
        .returns(new TypeHint<MergeGeoTuple>() {})
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator())
        .map(x-> {
          if (x.getValue().getLabel().startsWith("Ber"))
            LOG.info("SECOND: " + x.toString());
          return x;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .runOperation(new RepresentativeCreator(
            DataDomain.GEOGRAPHY,
            BlockingStrategy.STANDARD_BLOCKING));

//    plusDbp.print();
//    LOG.info(plusDbp.count());

    DataSet<MergeGeoTriplet> tupleResult = plusDbp
        .union(reps.filter(new SourceFilterFunction(Constants.FB_NS)))
//        .filter(vertex -> vertex.getValue().getBlockingKey().equals("ber"))
        .runOperation(new CandidateCreator(
            BlockingStrategy.STANDARD_BLOCKING,
            DataDomain.GEOGRAPHY,
            Constants.COSINE_TRIGRAM,
            Constants.FB_NS,
            4,
            env));

    DataSet<MergeGeoTriplet> singleEntities = tupleResult.join(tupleResult)
        .where(0)
        .equalTo(1)
        .with((left, right) -> left)
        .returns(new TypeHint<MergeGeoTriplet>() {
        })
        .distinct(0, 1);

    DataSet<Vertex<Long, ObjectMap>> plusFb = tupleResult
        .flatMap(new DualMergeGeographyMapper(false))
        .leftOuterJoin(baseClusters)
        .where(0)
        .equalTo(0)
        .with(new FinalMergeGeoVertexCreator());

//    plusFb.print();
    LOG.info(singleEntities.count());
    LOG.info(plusFb.count());
  }

  @Test
  public void incCountDataSourceElementsTest() throws Exception {
    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.MINSIZE)
        .build();

    String graphPath = IncrementalClusteringTest.class
//        .getResource("/data/preprocessing/oneToMany").getFile();
        .getResource("/data/geography").getFile();
    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class)
            .run(clustering);

    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      ObjectMap properties = vertex.getValue();

      if (properties.getDataSource().equals(Constants.GN_NS)) {
        assertEquals(749L, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.NYT_NS)) {
        assertEquals(755, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.DBP_NS)) {
        assertEquals(774, properties.getDataSourceEntityCount().longValue());
      } else if (properties.getDataSource().equals(Constants.FB_NS)) {
        assertEquals(776, properties.getDataSourceEntityCount().longValue());
      }
    }
  }

  private static class VertexFilterFunction extends RichFilterFunction<Vertex<Long, ObjectMap>> {
    private Set<Long> containSet;

    VertexFilterFunction(Set<Long> containSet) {
      this.containSet = containSet;
    }

    @Override
    public boolean filter(Vertex<Long, ObjectMap> value) throws Exception {
      return containSet.contains(value.getId());
    }
  }

//  @Test
//  public void splitCreation() throws Exception {
//    String graphPath = IncrementalClusteringTest.class
//        .getResource("/data/geography").getFile();
//    Graph<Long, ObjectMap, NullValue> graph =
//        new JSONDataSource(graphPath, true, env)
//            .getGraph(ObjectMap.class, NullValue.class);
//
//    FilterOperator<Vertex<Long, ObjectMap>> verts = graph
//        .getVertices()
//        .filter(new SourceFilterFunction(Constants.FB_NS));
//
//    List<Vertex<Long, ObjectMap>> vertices = verts.collect();
//    Collections.shuffle(vertices);
//
//    String first80gn = "80-db: ";
//    String add10gn = "10-add-db: ";
//    String final10gn = "10-final-db: ";
//    int counter = 0;
//    int all = 0;
//    int first = 0;
//    int add = 0;
//    int rest = 0;
//    for (Vertex<Long, ObjectMap> vertex : vertices) {
//      if (counter <= 7) {
//        first80gn = first80gn.concat(vertex.getId().toString())
//            .concat("L, ");
//        ++first;
//      } else if (counter == 8) {
//        add10gn = add10gn.concat(vertex.getId().toString())
//            .concat("L, ");
//        ++add;
//      } else if (counter == 9) {
//        final10gn = final10gn.concat(vertex.getId().toString())
//            .concat("L, ");
//        ++rest;
//      }
//
//      ++counter;
//      ++all;
//      if (counter == 10) {
//        counter = 0;
//      }
//    }
//
//    LOG.info(first80gn);
//    LOG.info(add10gn);
//    LOG.info(final10gn);
//    LOG.info(first + " " + add + " " + rest + " " + all);
//  }
}