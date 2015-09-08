package com.warriorkitty.orientlens;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;


public class ImportTest {

    private static final Logger logger = LoggerFactory.getLogger(ImportTest.class);

    public static final String PROPERTY_NAME_DATABASE_URL = "database.url";

    public static final String PROPERTY_NAME_DATABASE_USERNAME = "database.user";

    public static final String PROPERTY_NAME_DATABASE_PASSWORD = "database.password";

    public static final String PROPERTY_NAME_DATABASE_POOL_MIN = "database.pool.min";

    public static final String PROPERTY_NAME_DATABASE_POOL_MAX = "database.pool.max";

    public static final String PROPERTY_NAME_MOVIELENS_PATH = "movielens.path";

    private Properties properties;


    @BeforeClass
    @Parameters("properties.filename")
    public void initialize(String propertiesFilename) throws Exception {
        // Read and initialize properties.
        InputStream propertiesFileInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFilename);
        Assert.assertNotNull(propertiesFileInputStream);
        properties = new Properties();
        properties.load(propertiesFileInputStream);
        // Verify all necessary properties have been loaded.
        Assert.assertNotNull(properties.getProperty(PROPERTY_NAME_DATABASE_URL));
        Assert.assertNotNull(properties.getProperty(PROPERTY_NAME_DATABASE_USERNAME));
        Assert.assertNotNull(properties.getProperty(PROPERTY_NAME_DATABASE_PASSWORD));
        Assert.assertNotNull(properties.getProperty(PROPERTY_NAME_DATABASE_POOL_MIN));
        Assert.assertNotNull(properties.getProperty(PROPERTY_NAME_DATABASE_POOL_MAX));
        Assert.assertNotNull(properties.getProperty(PROPERTY_NAME_MOVIELENS_PATH));
    }


    @Test
    public void importTest() {
        Main.main(
                new String[] {
                        properties.getProperty(PROPERTY_NAME_DATABASE_URL),
                        properties.getProperty(PROPERTY_NAME_DATABASE_USERNAME),
                        properties.getProperty(PROPERTY_NAME_DATABASE_PASSWORD),
                        properties.getProperty(PROPERTY_NAME_DATABASE_POOL_MIN),
                        properties.getProperty(PROPERTY_NAME_DATABASE_POOL_MAX),
                        properties.getProperty(PROPERTY_NAME_MOVIELENS_PATH)
                }
        );
    }


}