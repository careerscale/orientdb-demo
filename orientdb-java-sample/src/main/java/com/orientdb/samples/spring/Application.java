package com.orientdb.samples.spring;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner.Mode;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * @author Harinath
 */
@SpringBootApplication
@EnableWebMvc
@Configuration
@ComponentScan("com.orientdb.samples.*")
public class Application implements CommandLineRunner {

    /** Reference to logger */
    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    @Autowired
    BaseDao baseDao;

    public static void main(final String[] args) {

        final SpringApplication application = new SpringApplication(Application.class);
        final Properties properties = new Properties();
        application.setBannerMode(Mode.CONSOLE);
        application.setDefaultProperties(properties);
        application.run();
    }

    @Override
    public void run(final String... arg0) throws Exception {


    }

}
