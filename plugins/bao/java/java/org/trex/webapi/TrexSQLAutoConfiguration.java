package org.trex.webapi;

import org.ohdsi.vocabulary.SearchProvider;
import org.ohdsi.webapi.plugins.WebApiPlugin;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import jakarta.servlet.http.HttpServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@ConditionalOnProperty(name = "trexsql.enabled", havingValue = "true")
public class TrexSQLAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TrexSQLAutoConfiguration.class);

    @Bean
    public WebApiPlugin trexsqlPlugin() throws Exception {
        return (WebApiPlugin) Class.forName("org.trex.webapi.TrexSQLPlugin")
            .getDeclaredConstructor().newInstance();
    }

    @Bean
    public SearchProvider trexsqlSearchProvider(Environment env) throws Exception {
        return (SearchProvider) Class.forName("org.trex.webapi.TrexSQLSearchProvider")
            .getDeclaredConstructor(Environment.class).newInstance(env);
    }

    @Bean
    public ServletRegistrationBean<HttpServlet> trexServlet(
            Environment env, ApplicationContext ctx) throws Exception {
        String cachePath = env.getProperty("trexsql.cache-path", "./data/cache");
        String extensionsPath = env.getProperty("trexsql.extensions-path", "");

        Map<String, Object> config = new HashMap<>();
        config.put("cache-path", cachePath);
        if (extensionsPath != null && !extensionsPath.isEmpty()) {
            config.put("extensions-path", extensionsPath);
        }
        config.put("allow-unsigned-extensions", true);

        // Initialize TrexSQL engine via reflection (class from Clojure AOT)
        Class<?> trexClass = Class.forName("org.trex.Trexsql");
        trexClass.getMethod("init", Map.class).invoke(null, config);
        log.info("TrexSQL engine initialized with cache-path: {}", cachePath);

        // Create and configure servlet via reflection
        Class<?> servletClass = Class.forName("org.trex.TrexServlet");
        HttpServlet servlet = (HttpServlet) servletClass.getDeclaredConstructor().newInstance();

        // Look up SourceRepository by type (avoids compile-time dependency on WebAPI)
        Class<?> repoClass = Class.forName("org.ohdsi.webapi.source.SourceRepository");
        Object sourceRepo = ctx.getBean(repoClass);

        Map<String, Object> servletConfig = new HashMap<>();
        servletConfig.put("cache-path", cachePath);

        servletClass.getMethod("initTrex", Object.class, Map.class)
            .invoke(servlet, sourceRepo, servletConfig);

        ServletRegistrationBean<HttpServlet> reg =
            new ServletRegistrationBean<>(servlet, "/trexsql/*");
        reg.setLoadOnStartup(1);
        log.info("TrexSQL servlet registered at /trexsql/*");
        return reg;
    }
}
