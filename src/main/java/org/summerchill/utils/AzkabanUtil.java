package org.summerchill.utils;

import java.util.HashSet;
import java.util.Iterator;

/**
 * Azakban's method that satisfy business
 */
public class AzkabanUtil {

    public static String set2String(HashSet<String> stringHashSet){
        StringBuilder sb = new StringBuilder("[");
        // 获取set集合数据
        for (Iterator iterator = stringHashSet.iterator(); iterator.hasNext();) {
            String nodeName = (String) iterator.next();
            sb.append("{\"nestedId\":\""+ nodeName + "\"},");
        }
        String jobs = sb.toString();
        return jobs.substring(0,jobs.length() - 1) + "]";
    }
}
