package com.pluralsight.course;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilterMovies {

    public static void main(String[] args) throws Exception {

        //this object will allow us to access flink functionality for processing datasets
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        // 2 . Need to convert CSV records to Java Objects
        // to convert objects from one type to another need to use map method
        // MapFunction to define how to transform every element in a regional dataset

        DataSet<Movie> movies = lines.map(new MapFunction<Tuple3<Long, String, String>, Movie>() {
            @Override
            public Movie map(Tuple3<Long, String, String> csvLine) throws Exception {
                String movieName = csvLine.f1;
                String[] genres = csvLine.f2.split("\\|");
                return new Movie(movieName, new HashSet<>(Arrays.asList(genres)));
            }
        });

        // 3 . filter movies with genres that contains "Drama"

        DataSet<Movie> filteredMovies = movies.filter(new FilterFunction<Movie>() {
            @Override
            public boolean filter(Movie movie) throws Exception {
                return movie.getGenres().contains("Drama");
            }
        });

        filteredMovies.writeAsText("output");

        // 4 .  up until this stage we have build the processing plan , now we need to execute it

        env.execute();
    }
}

class Movie {
    private String name;
    private Set<String> genres;

    public Movie(String name, Set<String> genres) {
        this.name = name;
        this.genres = genres;
    }

    public String getName() {
        return name;
    }

    public Set<String> getGenres() {
        return genres;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "name='" + name + '\'' +
                ", genres=" + genres +
                '}';
    }
}