package com.orientdb.samples.spring;



import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Scope;



/**
 *
 * @author Team Innominds
 */

@org.springframework.context.annotation.Configuration
@ComponentScan("com.orientdb.samples.spring.*")
public class OrientDbConfig {

    /** Reference to logger */
    private static final Logger LOG = LoggerFactory.getLogger(OrientDbConfig.class);

    @Value("${orientdb.uri}")
    private String uri;

    @Value("${orientdb.username}")
    private String username;

    @Value("${orientdb.password}")
    private String password;

    @Value("${orientdb.pool.minsize}")
    private Integer poolMinsize;

    @Value("${orientdb.pool.maxsize}")
    private Integer poolMaxsize;

    private OrientGraphFactory orientGraphFactory;

    @Bean
    public OrientGraphFactory getFactory() {
        orientGraphFactory = new OrientGraphFactory(uri, username, password);
        return orientGraphFactory.setupPool(poolMinsize, poolMaxsize);
    }

    @Bean
    @Scope("prototype")
    public OrientGraph getOrientGraph() {
        LOG.info("getOrientGraph");
        // EVERY TIME YOU NEED A GRAPH INSTANCE
        return getFactory().getTx();
    }

}
