package io.iftech.sparkudf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class TestUtils {

    public static String getStringFromFile(String filename) {
        InputStreamReader reader = new InputStreamReader(
            TestUtils.class.getResourceAsStream(filename),
            StandardCharsets.UTF_8
        );

        return new BufferedReader(reader).lines()
            .collect(Collectors.joining(System.lineSeparator()));
    }

    public static List<Object> convertSeqToList(Seq<Object> seq) {
        return JavaConverters.seqAsJavaList(seq);
    }
}
