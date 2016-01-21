package com.jfinal.ext.plugin.tomcatjdbc;


import com.jfinal.kit.StrKit;
import com.jfinal.plugin.IPlugin;
import com.jfinal.plugin.activerecord.IDataSourceProvider;
import org.apache.tomcat.jdbc.pool.DataSource;

/**
 * tomcat jdbc pool plugin
 * Created by moyu on 15/11/9.
 */
public class TomcatDataSourcePlugin  implements IPlugin, IDataSourceProvider {
    // 基本属性 url、user、password
    private String url;
    private String username;
    private String password;
    private String driverClass = "com.mysql.jdbc.Driver";

    // 初始连接池大小、最小空闲连接数、最大活跃连接数
    private int initialSize = 10;
    private int minIdle = 10;
    private int maxActive = 100;

    // 最长等待时间
    private int maxWait = 10000;

    // 配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
    private int timeBetweenEvictionRunsMillis = 30 * 1000;
    // 配置连接在池中最小生存的时间
    private int minEvictableIdleTimeMillis = 30 * 1000;

    private int validationInterval = 30 * 1000;

    /**
     * hsqldb - "select 1 from INFORMATION_SCHEMA.SYSTEM_USERS"
     * Oracle - "select 1 from dual"
     * DB2 - "select 1 from sysibm.sysdummy1"
     * mysql - "select 1"
     */
    private String validationQuery = "select 1";
    private boolean testWhileIdle = true;
    private boolean testOnBorrow = false;
    private boolean testOnReturn = false;

    // 是否打开连接泄露自动检测
    private boolean removeAbandoned = false;
    // 连接长时间没有使用，被认为发生泄露时长
    private int removeAbandonedTimeoutSecond = 60;
    // 发生泄露时是否需要输出 ltomcatog，建议在开启连接泄露检测时开启，方便排错
    private boolean logAbandoned = false;

    /**
     * Tomcat中的JDBC数据源对象
     */
    private DataSource ds;


    public TomcatDataSourcePlugin(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public TomcatDataSourcePlugin(String url, String username, String password, String driverClass) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClass = driverClass != null ? driverClass : this.driverClass;
    }


    public DataSource getDataSource() {
        return ds;
    }

    public boolean start() {
        ds = new DataSource();
        ds.setUrl(url);
        ds.setDriverClassName(driverClass);
        ds.setUsername(username);
        ds.setPassword(password);

        ds.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;" + "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
        ds.setMinIdle(minIdle);
        ds.setMaxActive(maxActive);
        ds.setInitialSize(initialSize);
        ds.setMaxWait(maxWait);


        ds.setJmxEnabled(true);


        ds.setValidationInterval(validationInterval);
        ds.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        ds.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);

        ds.setValidationQuery(validationQuery);
        ds.setTestOnReturn(testOnReturn);
        ds.setTestWhileIdle(testWhileIdle);
        ds.setTestOnBorrow(testOnBorrow);

        ds.setRemoveAbandoned(removeAbandoned);
        ds.setRemoveAbandonedTimeout(removeAbandonedTimeoutSecond);
        ds.setLogAbandoned(logAbandoned);

        return true;
    }

    public boolean stop() {
        if (ds != null) {
            ds.close();
        }
        return true;
    }

    public TomcatDataSourcePlugin setDriverClass(String driverClass) {
        if (StrKit.isBlank(driverClass))
            throw new IllegalArgumentException("driverClass can not be blank.");
        this.driverClass = driverClass;
        return this;
    }

    public TomcatDataSourcePlugin setInitialSize(int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public TomcatDataSourcePlugin setMinIdle(int minIdle) {
        this.minIdle = minIdle;
        return this;
    }

    public TomcatDataSourcePlugin setMaxActive(int maxActive) {
        this.maxActive = maxActive;
        return this;
    }

    public TomcatDataSourcePlugin setMaxWait(int maxWait) {
        this.maxWait = maxWait;
        return this;
    }

    public TomcatDataSourcePlugin setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
        return this;
    }

    public TomcatDataSourcePlugin setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
        return this;
    }

    public TomcatDataSourcePlugin setValidationInterval(int validationInterval) {
        this.validationInterval = validationInterval;
        return this;
    }

    public TomcatDataSourcePlugin setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
        return this;
    }

    public TomcatDataSourcePlugin setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
        return this;
    }

    public TomcatDataSourcePlugin setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
        return this;
    }

    public TomcatDataSourcePlugin setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
        return this;
    }

    public TomcatDataSourcePlugin setRemoveAbandoned(boolean removeAbandoned) {
        this.removeAbandoned = removeAbandoned;
        return this;
    }

    public TomcatDataSourcePlugin setRemoveAbandonedTimeoutSecond(int removeAbandonedTimeoutSecond) {
        this.removeAbandonedTimeoutSecond = removeAbandonedTimeoutSecond;
        return this;
    }

    public TomcatDataSourcePlugin setLogAbandoned(boolean logAbandoned) {
        this.logAbandoned = logAbandoned;
        return this;
    }

    public TomcatDataSourcePlugin setDs(DataSource ds) {
        this.ds = ds;
        return this;
    }
}