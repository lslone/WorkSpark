package com.corey.streamingframework.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class SecurityUtils {
    public static Log log  = LogFactory.getLog(SecurityUtils.class);
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME ="client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY ="zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";
    private static Configuration conf = null;

    private static Configuration getConf(){
        if (null == conf){
            conf = new Configuration();
        }
        return conf;
    }

    public static void login(String userKeytabFile,String krb5File,String userName) throws IOException {
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME,userName,userKeytabFile);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(userName,userKeytabFile,krb5File,getConf());
    }

    public static void login(String userKeytabFile,String krb5File,String userName,Configuration conf) throws IOException {
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME,userName,userKeytabFile);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
        LoginUtil.login(userName,userKeytabFile,krb5File,conf);
    }

    public static Boolean relogin(){
        boolean flag = false;
        try {
            UserGroupInformation.getLoginUser().reloginFromKeytab();
            log.info("UserGroupInformation.isLoginKeytabBased(): " + UserGroupInformation.isLoginKeytabBased());
            flag = true;
        } catch (IOException e){
            log.error(e.getMessage(),e);
        }
        return flag;
    }


}
