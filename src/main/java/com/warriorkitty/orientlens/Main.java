package com.warriorkitty.orientlens;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {

        // the best way to get a Graph instance is through the OrientGraphFactory
        OrientGraphFactory factory =
                new OrientGraphFactory(Config.DB_URL, Config.DB_USERNAME, Config.DB_PASSWORD)
                        .setupPool(Config.DB_POOL_MIN, Config.DB_POOL_MAX);

        // gets transactional graph
        OrientGraph graph = factory.getTx();

        Logger.log("Database initialized.");

        // Worker is a custom class which does all the work
        Worker worker = new Worker(graph);

        Logger.log("Worker initialized.");

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
