package com.warriorkitty.orientlens;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

// custom class which does most of the work
// it's called mostly from Main.java
public class Worker {

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private String movieLensPath;

    // graph instance
    public OrientGraph graph;

    public Worker(OrientGraph graph, String movieLensPath) {
        this.graph = graph;
        this.movieLensPath = movieLensPath;
    }

    public void addGenres() throws IOException {

        logger.info("Adding Genres.");

        // get lines as stream
        Stream<String> lines = Files.lines(Paths.get(movieLensPath + "movies.csv"));

        // set will always have unique values
        Set<String> genres = new HashSet<>();

        // genres format: someMovie,genre1|genre2|genre3
        lines.skip(1).forEach(line -> {
            // split by comma (commas inside double quotes needs to be skipped)
            String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            genres.addAll(Arrays.asList(tokens[2].split("\\|")));
        });

        logger.info("Genres loaded from CSV into HashSet.");

        // create a class
        // execute it outside a transaction because this class needs to be created before adding genres
        graph.executeOutsideTx(arg -> {

            // drop if exist
            if (graph.getVertexType("Genre") != null) {
                graph.dropVertexType("Genre");
            }

            // creating the class
            OrientVertexType genreClass = graph.createVertexType("Genre");
            genreClass.createProperty("name", OType.STRING);

            // creating the type
            OrientVertexType genreType = graph.getVertexType("Genre");
            genreType.createIndex("Genre.name", OClass.INDEX_TYPE.UNIQUE, "name");

            return null;
        });

        // add vertices
        genres.stream().forEach(genre -> {
            try {
                graph.addVertex("class:Genre", "name", genre);
            } catch (Exception e) {
                graph.rollback();
            }
        });
        graph.commit();

        logger.info("Genres added. [{}]", graph.countVertices("Genre"));

    }

    public void addUsers() throws IOException {

        logger.info("Adding Users.");

        // get lines as stream
        Stream<String> lines = Files.lines(Paths.get(movieLensPath + "ratings.csv"));

        // set will always have unique values
        Set<Integer> users = new HashSet<>();

        lines.skip(1).forEach(line -> {
            // split by comma
            String[] tokens = line.split(",");
            users.add(Integer.parseInt(tokens[0]));
        });

        logger.info("Users loaded from CSV into HashSet.");

        // create a class
        graph.executeOutsideTx(iArgument -> {

            // drop if exist
            if (graph.getVertexType("User") != null) {
                graph.dropVertexType("User");
            }

            OrientVertexType userClass = graph.createVertexType("User");
            userClass.createProperty("userId", OType.INTEGER);
            OrientVertexType genreType = graph.getVertexType("User");
            genreType.createIndex("User.userId", OClass.INDEX_TYPE.UNIQUE, "userId");
            return null;
        });

        // add vertices
        users.stream().forEach(id -> {
            try {
                graph.addVertex("class:User", "userId", id);
                // worked faster for me
                if (Worker.randInt(1, 20) == 5) {
                    graph.commit();
                }
            } catch (Exception e) {
                graph.rollback();
            }
        });
        graph.commit();

        logger.info("Users added. [{}]", graph.countVertices("User"));
    }

    public void addMovies() throws IOException {

        logger.info("Adding Movies.");

        // get lines as stream
        Stream<String> lines = Files.lines(Paths.get(movieLensPath + "movies.csv"));

        // set will always have unique values
        Map<Integer, String> movies = new HashMap<>();

        lines.skip(1).forEach(line -> {
            // split by comma (commas inside double quotes needs to be skipped)
            String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String movie = tokens[1].replaceAll("\\\"", "");
            Integer movieId = Integer.parseInt(tokens[0]);
            movies.put(movieId, movie);
        });

        logger.info("Movies loaded from CSV into HashMap.");

        // create a class
        // execute it outside a transaction because this class needs to be created before adding movies
        graph.executeOutsideTx(iArgument -> {

            // drop if exist
            if (graph.getVertexType("Movie") != null) {
                graph.dropVertexType("Movie");
            }

            OrientVertexType movieClass = graph.createVertexType("Movie");
            movieClass.createProperty("name", OType.STRING);
            movieClass.createProperty("movieId", OType.INTEGER);

//            OrientVertexType genreType = graph.getVertexType("Movie");
//            genreType.createIndex("Movie.name", OClass.INDEX_TYPE.UNIQUE, "name");

            OrientVertexType movieIdType = graph.getVertexType("Movie");
            movieIdType.createIndex("Movie.movieId", OClass.INDEX_TYPE.UNIQUE, "movieId");

            return null;
        });

        // add vertices
        movies.entrySet().stream().forEach(movie -> {
            try {
                graph.addVertex("class:Movie", "name", movie.getValue(), "movieId", movie.getKey());
                // worked faster for me
                if (Worker.randInt(1, 20) == 5) {
                    graph.commit();
                }
            } catch (Exception e) {
                graph.rollback();
            }
        });
        graph.commit();

        logger.info("Movies added. [{}]", graph.countVertices("Movie"));
    }

    public void connectMoviesWithGenres() throws IOException {
        logger.info("Connecting Movies with Genres.");
        // get lines as stream
        Stream<String> lines = Files.lines(Paths.get(movieLensPath + "movies.csv"));

        // could go forEachOrdered but it doesn't matter in this case
        lines.skip(1).forEach(line -> {
            // split by comma
            // commas inside double quotes needs to be skipped
            String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String[] genres = tokens[2].split("\\|");
            Integer movieId = Integer.parseInt(tokens[0]);

            // result set
            Iterable<OrientVertex> moviesRs = graph.command(new OSQLSynchQuery<OrientVertex>(
                    String.format("select from Movie where movieId = %d", movieId)
            )).execute();

            if (moviesRs.iterator().hasNext()) {
                OrientVertex movieVertex = moviesRs.iterator().next();
                Arrays.stream(genres).forEach(genre -> {
                    // result set
                    Iterable<OrientVertex> genresRs = graph.command(new OSQLSynchQuery<OrientVertex>(
                            String.format("select from Genre where name = \"%s\"", genre)
                    )).execute();
                    OrientVertex genreVertex = genresRs.iterator().next();

                    graph.addEdge(null, movieVertex, genreVertex, "is_genre");
                });

                // worked faster for me
                if (Worker.randInt(1, 20) == 5) {
                    graph.commit();
                }
            }

        });

        graph.commit();

        logger.info("Movies and Genres are connected. [{}]", graph.countEdges("is_genre"));
    }

    public void rateMovies() throws IOException {
        logger.info("Connecting Movies with Users (rates).");

        graph.executeOutsideTx(arg -> {

            // drop if exist
            if (graph.getEdgeType("Rate") != null) {
                graph.dropEdgeType("Rate");
            }

            // creating the edge class
            OrientEdgeType edgeClass = graph.createEdgeType("Rate");
            edgeClass.createProperty("rating", OType.FLOAT);
            edgeClass.createProperty("timestamp", OType.INTEGER);

            return null;
        });

        // get lines as stream
        Stream<String> lines = Files.lines(Paths.get(movieLensPath + "ratings.csv"));

        lines.skip(1).forEach(line -> {
            // split by comma
            String[] tokens = line.split(",", -1);
            Integer userId = Integer.parseInt(tokens[0]);
            Integer movieId = Integer.parseInt(tokens[1]);
            Float rating = Float.parseFloat(tokens[2]);
            Integer timestamp = Integer.parseInt(tokens[3]);

            Iterable<OrientVertex> moviesRs = graph.command(new OSQLSynchQuery<OrientVertex>(
                    String.format("select from Movie where movieId = %d", movieId)
            )).execute();

            Iterable<OrientVertex> userRs = graph.command(new OSQLSynchQuery<OrientVertex>(
                    String.format("select from User where userId = %d", userId)
            )).execute();

            if (moviesRs.iterator().hasNext() && userRs.iterator().hasNext()) {
                OrientVertex movieVertex = moviesRs.iterator().next();
                OrientVertex userVertex = userRs.iterator().next();
                userVertex.addEdge("Rate", movieVertex, new Object[] {"rating", rating, "timestamp", timestamp});
            }

            // worked faster for me
            if (Worker.randInt(1, 20) == 5) {
                graph.commit();
            }
        });

        graph.commit();
        logger.info("Movies and Users are connected. (rates) [{}]", graph.countEdges("Rate"));
    }

    public void tagMovies() throws IOException {
        logger.info("Tagging movies.");

        graph.executeOutsideTx(arg -> {

            // drop if exist
            if (graph.getEdgeType("Tag") != null) {
                graph.dropEdgeType("Tag");
            }

            // creating the edge class
            OrientEdgeType edgeClass = graph.createEdgeType("Tag");
            edgeClass.createProperty("tag", OType.STRING);
            edgeClass.createProperty("timestamp", OType.INTEGER);

            return null;
        });

        // get lines as stream
        Stream<String> lines = Files.lines(Paths.get(movieLensPath + "tags.csv"));

        lines.skip(1).forEach(line -> {
            // split by comma
            String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            Integer userId = Integer.parseInt(tokens[0]);
            Integer movieId = Integer.parseInt(tokens[1]);
            String tag = tokens[2].replaceAll("\\\"", "");
            Integer timestamp = Integer.parseInt(tokens[3]);

            Iterable<OrientVertex> moviesRs = graph.command(new OSQLSynchQuery<OrientVertex>(
                    String.format("select from Movie where movieId = %d", movieId)
            )).execute();

            Iterable<OrientVertex> userRs = graph.command(new OSQLSynchQuery<OrientVertex>(
                    String.format("select from User where userId = %d", userId)
            )).execute();

            if (moviesRs.iterator().hasNext() && userRs.iterator().hasNext()) {
                OrientVertex movieVertex = moviesRs.iterator().next();
                OrientVertex userVertex = userRs.iterator().next();
                userVertex.addEdge("Tag", movieVertex, new Object[] {"tag", tag, "timestamp", timestamp});
            }

            // worked faster for me
            if (Worker.randInt(1, 20) == 5) {
                graph.commit();
            }
        });

        graph.commit();
        logger.info("Tagging finished. [{}]", graph.countEdges("Tag"));
    }

    public static int randInt(int min, int max) {
        Random rand = new Random();
        return rand.nextInt((max - min) + 1) + min;
    }


}