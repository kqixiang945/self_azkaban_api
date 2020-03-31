package org.summerchill.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

@Slf4j
public class HttpUtil {
    /**
     * @param urlStr          Post的请求地址Url
     * @param postContentType Post请求的类型
     * @param postStr
     * @return
     */
    public static String sendPost(String urlStr, String postContentType, String postStr, List<NameValuePair> param ) {
        StringBuffer result = new StringBuffer();
        try {
            HttpClient httpClient = getHttpClient();
            HttpPost request = new HttpPost(urlStr);
            if (Constant.HTTP_POST_CONTENT_TYPE_JSON.equals(postContentType)) {
                JSONObject jsonParam = new JSONObject(postStr);
                request.setEntity(new StringEntity(jsonParam.toString(), Consts.UTF_8));
            } else if (Constant.HTTP_POST_CONTENT_TYPE_URLENCODE.equals(postContentType)) {
                request.setHeader("Content-Type",postContentType);
                request.setEntity(new UrlEncodedFormEntity(param, Consts.UTF_8));
            }
            HttpResponse response = httpClient.execute(request);
            if (response != null) {
                int statusCode = response.getStatusLine().getStatusCode();
                //System.out.println("Http Response Code : " + statusCode);
                //deal with redirect condition
                if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY) {
                    Header locationHeader = response.getLastHeader("location");
                    if (locationHeader != null) {
                        //get redirect url
                        String location = locationHeader.getValue();
                        HttpUtil.sendPost(location,Constant.HTTP_POST_CONTENT_TYPE_URLENCODE,"",param);
                    }
                }else if (statusCode == HttpStatus.SC_OK) {
                    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                    String line = "";
                    //notice the rd may contains lots of lines
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                    //System.out.println("查询的URL是:" + urlStr + "; 对应的查询参数为:" + param + ";对应的返回值为:" + result);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return result.toString();
    }

    /**
     * 发送HttpGet请求
     * @param url
     * @return
     */
    public static String sendGet(String url,Map<String,String> headerMap) {
        //1.获得一个httpclient对象
        CloseableHttpClient httpclient = getHttpClient();
        //2.生成一个get请求
        HttpGet httpget = new HttpGet(url);
        if(headerMap != null){
            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                httpget.setHeader(entry.getKey(),entry.getValue());
            }
        }
        CloseableHttpResponse response = null;
        try {
            //3.执行get请求并返回结果
            response = httpclient.execute(httpget);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        String result = null;
        try {
            //4.处理结果，这里将结果返回为字符串
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                result = EntityUtils.toString(entity,"UTF-8");
            }
        } catch (ParseException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }


    public static CloseableHttpClient getHttpClient() {
        try {
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            //不进行主机名验证
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(builder.build(),
                    NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory> create()
                    .register("http", new PlainConnectionSocketFactory())
                    .register("https", sslConnectionSocketFactory)
                    .build();

            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
            cm.setMaxTotal(100);
            CloseableHttpClient httpclient = HttpClients.custom()
                    .setSSLSocketFactory(sslConnectionSocketFactory)
                    .setDefaultCookieStore(new BasicCookieStore())
                    .setConnectionManager(cm).build();
                    //.setRedirectStrategy(new LaxRedirectStrategy()).build();
            return httpclient;
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        }
        return HttpClients.createDefault();
    }
}