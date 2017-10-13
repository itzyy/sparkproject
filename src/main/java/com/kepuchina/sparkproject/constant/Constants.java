package com.kepuchina.sparkproject.constant;

/**
 * 常量接口
 */
public interface Constants {

    /**
     * 项目配置的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USERNAME = "jdbc.username";
    String JDBC_PASSWORD = "jdbc.password";

    /**
     * spark相关常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
    String SPARK_LOCAL = "spark.local";
    String FIELD_SESSION_ID="sessionId";
    String FIELD_SEARCH_KEYWORDS="searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS="clickCategoryIds";
    String FIELD_USERINFO_AGE="age";
    String FIELD_USERINFO_PROFESSIONAL="professional";
    String FIELD_USERINFO_CITY="city";
    String FIELD_USERINFO_SEX="sex";
    /**
     * 任务相关常量
     */
    String PARAM_START_DATE="startDate";
    String PARAM_END_DATE="endDate";


}
