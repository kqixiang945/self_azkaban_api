package org.summerchill.utils;

public class Constant {
    //发送HTTP的POST请求的类型
    public static final String HTTP_POST_CONTENT_TYPE_JSON = "application/json";
    public static final String HTTP_POST_CONTENT_TYPE_URLENCODE = "application/x-www-form-urlencoded";
    //Azkaban地址前缀
    public static final String URL_PART1 = "https://******:8443/";
    //Azakban的Job运行的状态
    public static final String SUCCESSED = "SUCCEEDED";
    public static final String SKIPPED = "SKIPPED";
    public static final String CANCELLED = "CANCELLED";
    public static final String FAILED = "FAILED";



    //==================统计hive元数据的mysql数据库连接=========================
    public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
    public static final String MYSQL_AZKABAN_URL = "jdbc:mysql://*****:3306/azkaban";
    public static final String MYSQL_AZKABAN_USERNAME = "*****";
    public static final String MYSQL_AZKABAN_PASSWORD = "*****";


    //Azkaban中任务的关系
    public static final String PARENTS = "Parents";
    public static final String CHILDREN = "Children";
    public static final String ANCESTORS = "Ancestors";
    public static final String DESCENDANTS = "Descendants";
}
