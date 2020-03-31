package org.summerchill.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ConnectionUtil {
    static Connection conn = null;
    static Statement stmt = null;
    static ResultSet rs = null;
    /**
     * 获取到Azkaban数据库的连接
     * @return
     */
    public static Connection loadAzkabanConnection() {
        try {
            log.info("开始获取Azkaban数据库的连接....");
            Class.forName(Constant.MYSQL_DRIVER_NAME);
            conn = DriverManager.getConnection(Constant.MYSQL_AZKABAN_URL, Constant.MYSQL_AZKABAN_USERNAME,
                    Constant.MYSQL_AZKABAN_PASSWORD);
            return conn;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ResultSet getDataFromAzkabanDb(String sql){
        try {
            conn = loadAzkabanConnection();
            log.info("要执行的sql为:" + sql);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            return rs;
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}