package com.warriorkitty.orientlens;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        String dbUrl = Config.DB_URL;
        String dbUsername = Config.DB_USERNAME;
        String dbPassword = Config.DB_PASSWORD;
        int dbPoolMin = Config.DB_POOL_MIN;
        int dbPoolMax = Config.DB_POOL_MAX;
        String movieLensPath = Config.MOVIELENS_PATH;

        if (args.length >= 1) { dbUrl = args[0]; }
        if (args.length >= 2) { dbUsername = args[1]; }
        if (args.length >= 3) { dbPassword = args[2]; }
        if (args.length >= 4) { dbPoolMin = Integer.parseInt(args[3]); }
        if (args.length >= 5) { dbPoolMax = Integer.parseInt(args[4]); }
        if (args.length >= 6) { movieLensPath = args[5]; }

        // the best way to get a Graph instance is through the OrientGraphFactory
        OrientGraphFactory factory =
                new OrientGraphFactory(dbUrl, dbUsername, dbPassword)
                        .setupPool(dbPoolMin, dbPoolMax);

        // gets transactional graph
        OrientGraph graph = factory.getTx();

        logger.info("Database initialized.");

        // Worker is a custom class which does all the work
        Worker worker = new Worker(graph, movieLensPath);

        logger.info("Worker initialized.");

        try {
            worker.addGenres();
            worker.addUsers();
            worker.addMovies();
            worker.connectMoviesWithGenres();
            worker.rateMovies();
            worker.tagMovies();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}