package com.kepuchina.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.kepuchina.sparkproject.config.ConfigurationManager;
import com.kepuchina.sparkproject.constant.Constants;
import com.kepuchina.sparkproject.dao.impl.DAOFactory;
import com.kepuchina.sparkproject.domain.Task;
import com.kepuchina.sparkproject.test.MockData;
import com.kepuchina.sparkproject.util.ParamUtils;
import com.kepuchina.sparkproject.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session分析spark作业
 * 接收用户创建的分析任务，用户可能指定的条件如下
 * 1、时间范围：起始日期、结束日期
 * 2、性别：男或女
 * 3、职业：多选
 * 4、城市：多选
 * 5、年龄范围
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * <p>
 * 我们的spark作业如何接受用户创建的任务？
 * <p>
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param
 * 字段中
 * <p>
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 * <p>
 * 这是spark本身提供的特性
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        //构建spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext context = new SQLContext(jsc.sc());
        //生成模拟数据
        mockData(jsc, context);

        //通过参数传递进行的taskid
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        // 通过taskid查找出对应的task
        Task task = DAOFactory.getTaskDao().findById(taskId);
        //将taskparam转换成json
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        //如果要进行session粒度的数据聚合

        // 首先要从user_visit_session表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDataRange(context, taskParam);
        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        JavaPairRDD<String, String> session2fullAggrInfoRDD = aggregateBySession(context, actionRDD);

        // 首先按照session_id对数据进行聚合，使用groupBy
        // 对用户信息进行join

        //关闭spark上下文
        jsc.close();

    }


    /**
     * 如果是本地允许则使用sqlcontext
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        if (ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 制造session数据
     *
     * @param jsc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext jsc, SQLContext sqlContext) {
        if (ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            MockData.mock(jsc, sqlContext);
        }
    }

    /**
     * 通过taskparam获取startData和endDate，过滤出相关的数据
     *
     * @param sqlContext
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDataRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, ConfigurationManager.getProperty(Constants.PARAM_START_DATE));
        String endDate = ParamUtils.getParam(taskParam, ConfigurationManager.getProperty(Constants.PARAM_END_DATE));
        String sql = "" +
                "select *" +
                " from user_visit_action " +
                " where date >= '" + startDate + "' and date<='" + endDate + "'";
        DataFrame df = sqlContext.sql(sql);
        return df.javaRDD();
    }

    /**
     * 使用session进行聚合
     *
     * @param context
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SQLContext context, JavaRDD<Row> actionRDD) {

        //将数据的格式映射成<sessionId,row>
        JavaPairRDD<String, Row> sessionMapRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                //<session_id,row>
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
        //通过sessionId对数据进行分组，数据格式为<sessionId,Iterable<Row>>
        JavaPairRDD<String, Iterable<Row>> sessionGroupRDD = sessionMapRDD.groupByKey();
        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userIdSeachClickRDD = sessionGroupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> str) throws Exception {
                String sessionId = str._1;
                Iterator<Row> iterator = str._2.iterator();
                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                Long userId = 0l;
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    String searchKeyword = row.getString(5);
                    Long clickCategoryId = row.getLong(6);

                    if (userId == null) {
                        userId = row.getLong(1);
                    }

                    //分别聚合搜索词
                    if (StringUtils.isNotEmpty(searchKeyword)) {
                        if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                            searchKeywordsBuffer.append(searchKeyword);
                        }
                    }
                    //聚合点击词
                    if (clickCategoryId != null) {
                        if (clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                            clickCategoryIdsBuffer.append(clickCategoryId);
                        }
                    }
                }
                String keywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clicks = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + keywords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clicks;

                return new Tuple2<Long, String>(userId, partAggrInfo);
            }
        });

        //查询所有用户数据，并映射成<userId,row>格式
        String sql ="select * from user_info";
        DataFrame userInfoDF = context.sql(sql);
        // 转换数据格式<userid,row>
        JavaPairRDD<Long, Row> pairUserInfoRDD = userInfoDF.javaRDD().mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });
        JavaPairRDD<Long, Tuple2<String, Row>> joinSessionUserInfoRDD = userIdSeachClickRDD.join(pairUserInfoRDD);

        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = joinSessionUserInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> value) throws Exception {
                Long userId = value._1;
                String partAggInfo = value._2._1;
                Row userInfoRow = value._2._2;

                String sessionID = StringUtils.getFieldFromConcatString(partAggInfo, "\\|", Constants.FIELD_SESSION_ID);
                Long age = userInfoRow.getLong(3);
                String professional = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggInfo + "|" + Constants.FIELD_USERINFO_AGE + "=" + age + "|" + Constants.FIELD_USERINFO_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_USERINFO_CITY + "=" + city + "|" + Constants.FIELD_USERINFO_SEX + "=" + sex;
                return new Tuple2<String, String>(sessionID, fullAggrInfo);
            }
        });
        return sessionid2FullAggrInfoRDD;

    }
}
