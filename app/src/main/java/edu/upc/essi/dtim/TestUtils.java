package edu.upc.essi.dtim;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.upc.essi.dtim.nextiaqr.jena.GraphOperations;
import edu.upc.essi.dtim.nextiaqr.utils.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

    public static Map<String,String> populatePrefixes(String filePath) throws IOException {
        Map<String,String> out = Maps.newHashMap();
        Files.readAllLines(new File(filePath).toPath()).forEach(p -> out.put(p.split(" ")[0],p.split(" ")[1]));
        return out;
    }

    public static void populateTriples(String jenaPath, String namedGraph, String filePath, Map<String,String> prefixes) throws IOException {
        Files.readAllLines(new File(filePath).toPath()).stream().filter(t -> !t.startsWith("#")).forEach(t -> {
            String[] spo = t.split(" ");
            String s = prefixes.get(spo[0].split(":")[0])+spo[0].split(":")[1];
            String p = prefixes.get(spo[1].split(":")[0])+spo[1].split(":")[1];
            String o = prefixes.get(spo[2].split(":")[0])+spo[2].split(":")[1];

            GraphOperations.getInstance(jenaPath).addTriple(namedGraph,s,p,o);
        });
    }

    public static void populateMappings(String jenaPath, String mappingsPath, String globalGraphPath, Map<String,String> prefixes) throws IOException {
        List<String> globalGraphTriples =  Files.readAllLines(new File(globalGraphPath).toPath());
        Files.readAllLines(new File(mappingsPath).toPath()).stream().filter(t -> !t.startsWith("#")).forEach(t -> {
            String wrapper = t.split("-")[0];
            String[] indexes = t.split("-")[1].split(",");
            Arrays.stream(indexes).map(i -> Integer.parseInt(i)).forEach(i -> {
                String spo[] = globalGraphTriples.get(i-1).split(" ");
                String s = prefixes.get(spo[0].split(":")[0])+spo[0].split(":")[1];
                String p = prefixes.get(spo[1].split(":")[0])+spo[1].split(":")[1];
                String o = prefixes.get(spo[2].split(":")[0])+spo[2].split(":")[1];

                GraphOperations.getInstance(jenaPath).addTriple(prefixes.get(wrapper.split(":")[0])+wrapper.split(":")[1],s,p,o);
            });
        });
    }

    public static List<Tuple2<String,String>> getQueries(String filePath, Map<String,String> prefixes) throws IOException {
        List<Tuple2<String,String>> queries = Lists.newArrayList();

        List<String> lines = Files.readAllLines(new File(filePath).toPath());
        String queryDesc = "";
        String query = "";
        boolean first = true;
        for (String line : lines) {
            if (line.startsWith("#")) {
                if (first) first = false;
                else queries.add(new Tuple2<String,String>(queryDesc,query));

                query = prefixes.keySet().stream().map(p -> "PREFIX "+p+": <"+prefixes.get(p)+"> ").reduce(String::concat).get();
                queryDesc = line;
            } else {
                query += line+" ";
            }
        }
        return queries;
    }

    public static HashMap<String,String> getFilesPerWrapper(String filePath) throws IOException {
        HashMap<String,String> filesPerWrapper = Maps.newHashMap();
        Files.readAllLines(new File(filePath).toPath()).forEach(line -> {
            filesPerWrapper.putIfAbsent(line.split(",")[0],line.split(",")[1]);
        });
        return filesPerWrapper;
    }

}
