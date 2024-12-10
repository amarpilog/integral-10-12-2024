package com.pilog.mdm;


import org.hibernate.SessionFactory;
import java.util.Properties;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariConfig;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Configuration;

import org.springframework.orm.hibernate5.HibernateTransactionManager;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableAutoConfiguration(exclude = { HibernateJpaAutoConfiguration.class })
@EnableTransactionManagement
public class DataSourceConfig
{
//    @Value("${jndiname}")
//    private String jndiName;
    @Value("${jdbc.driver}")
    private String driverClassName;
    @Value("${jdbc.url}")
    private String url;
    @Value("${jdbc.username}")
    private String userName;
    @Value("${jdbc.password}")
    private String userPwd;
    @Value("${hibernate.dialect}")
    private String hibernateDialect;
    @Value("${hibernate.show_sql}")
    private String hibernateShow_sql;
    @Value("${hibernate.format_sql}")
    private String hibernateFormat_sql;
    @Value("${hibernate.use_sql_comments}")
    private String hibernateUse_sql_comments;
    @Value("${hibernate.validator.apply_to_ddl}")
    private String hibernateValidatorApply_to_ddl;
    @Value("${hibernate.validator.autoregister_listeners}")
    private String hibernateValidatorAutoregister_listeners;
    @Value("${hibernate.generate_statistics}")
    private String hibernate_generate_statistics;
    @Value("${hibernate.jdbc.batch_size}")
    private String hibernate_jdbc_batch_size;
    @Value("${hibernate.connection.release_mode}")
    private String hibernate_connection_release_mode;
    @Value("${hibernate.search.default.directory_provider}")
    private String hibernateSearchDefaultDirectory_provider;
    @Value("${hibernate.search.default.indexBase}")
    private String hibernateSearchDefaultIndexBase;
    @Value("${MultipartResolver.fileUploadSize}")
    private long fileUploadSize;
    @Value("${MultipartResolver.fileinMemorySize}")
    private int fileinMemorySize;
    @Value("${hibernate.pool.max_size}")
    private String connPoolMaxSize;
    @Value("${hibernate.pool.min_size}")
    private String connPoolMinSize;
    @Value("${hibernate.pool.idle_test_period}")
    private String connPoolIdlePeriod;
    @Value("${hibernate.pool.minimumIdle}")
    private String poolMinimumIdle;
    @Value("${hibernate.pool.ConnectionTimeout}")
    private String connectionTimeout;
    @Value("${dataSouceClassName}")
    private String dataSouceClassName;
    
    @Bean(name = { "multipartResolver" })
    public CommonsMultipartResolver createMultipartResolver() {
        final CommonsMultipartResolver resolver = new CommonsMultipartResolver();
        resolver.setMaxUploadSize(this.fileUploadSize);
        resolver.setMaxInMemorySize(this.fileinMemorySize);
        return resolver;
    }
    
    @Bean(name = { "dataSourceBean" }, destroyMethod = "close")
    public DataSource dataSource() {
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(this.driverClassName);
        hikariConfig.setJdbcUrl(this.url);
        hikariConfig.setUsername(this.userName);
        hikariConfig.setPassword(this.userPwd);
        hikariConfig.setMaximumPoolSize(Integer.parseInt(this.connPoolMaxSize));
        hikariConfig.setIdleTimeout((long)Integer.parseInt(this.connPoolIdlePeriod));
        hikariConfig.setConnectionTimeout((long)Integer.parseInt(this.connectionTimeout));
        hikariConfig.setMinimumIdle(Integer.parseInt(this.poolMinimumIdle));
        hikariConfig.setPoolName("springHikariCP");
        final HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        return (DataSource)dataSource;
    }
    
    private Properties getHibernateProperties() {
        final Properties properties = new Properties();
        properties.put("hibernate.show_sql", this.hibernateShow_sql);
        properties.put("hibernate.dialect", this.hibernateDialect);
        properties.put("hibernate.format_sql", this.hibernateFormat_sql);
        properties.put("hibernate.use_sql_comments", this.hibernateUse_sql_comments);
        properties.put("hibernate.validator.apply_to_ddl", this.hibernateValidatorApply_to_ddl);
        properties.put("hibernate.validator.autoregister_listeners", this.hibernateValidatorAutoregister_listeners);
        properties.put("hibernate.search.default.directory_provider", this.hibernateSearchDefaultDirectory_provider);
        properties.put("hibernate.search.default.indexBase", this.hibernateSearchDefaultIndexBase);
        properties.put("hibernate.id.new_generator_mappings", false);
        properties.put("hibernate.jdbc.batch_size", this.hibernate_jdbc_batch_size);
        properties.put("hibernate.order_inserts", true);
        properties.put("hibernate.order_updates", true);
        properties.put("hibernate.generate_statistics", this.hibernate_generate_statistics);
        properties.put("hibernate.connection.release_mode", this.hibernate_connection_release_mode);
        return properties;
    }
    
    @Bean(name = { "sessionFactory" })
    public SessionFactory localSessionFactoryBean() throws Exception {
        final LocalSessionFactoryBean sessionFactoryBean = new LocalSessionFactoryBean();
        sessionFactoryBean.setDataSource(this.dataSource());
        sessionFactoryBean.setPackagesToScan(new String[] { "com.pilog.mdm.pojo" });
        sessionFactoryBean.setHibernateProperties(this.getHibernateProperties());
        sessionFactoryBean.afterPropertiesSet();
        return sessionFactoryBean.getObject();
    }
    
    @Bean(name = "transactionManager")
    public HibernateTransactionManager getTransactionManager() throws Exception {
        HibernateTransactionManager transactionManager = new HibernateTransactionManager();
        transactionManager.setSessionFactory(localSessionFactoryBean());
        return transactionManager;
    }
    
}