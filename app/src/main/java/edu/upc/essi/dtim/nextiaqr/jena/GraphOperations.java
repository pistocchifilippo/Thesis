package edu.upc.essi.dtim.nextiaqr.jena;
import com.google.common.collect.Lists;
import edu.upc.essi.dtim.nextiaqr.models.graph.CQVertex;
import edu.upc.essi.dtim.nextiaqr.models.graph.IntegrationGraph;
import edu.upc.essi.dtim.nextiaqr.models.metamodel.GlobalGraph;
import edu.upc.essi.dtim.nextiaqr.models.metamodel.Namespaces;
import edu.upc.essi.dtim.nextiaqr.utils.Tuple3;
import lombok.Getter;
import lombok.Setter;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.update.UpdateAction;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.List;

@Getter @Setter
public class GraphOperations {
    private Dataset ds;
    private Model model;

    public GraphOperations(String jenaPath){
        ds = TDBFactory.createDataset(jenaPath);
        model = ds.getDefaultModel();
        //model = ModelFactory.createDefaultModel();
    }

    public void add(String subject, String predicate, String object) {
        Resource r = model.createResource(subject);
        r.addProperty(model.createProperty(predicate), model.createResource(object));
    }

    public void add(String subject, Property predicate, Resource object) {
        Resource r = model.createResource(subject);
        r.addProperty(predicate, object);
    }

    public void add(String subject, Property predicate, String object) {
        Resource r = model.createResource(subject);
        r.addProperty(predicate, model.createResource(object));
    }

    public void deleteResource(String uri) {
        deleteSubject(uri);
        deleteObject(uri);
    }

    public void deleteSubject(String uri) {
        Resource r = model.createResource(uri);
        model.removeAll(r, null, null);
    }

    public void deleteObject(String uri) {
        Resource r = model.createResource(uri);
        model.removeAll(null, null, r);
    }

    public void delete(String subject, String predicate, String object){
        model.removeAll(new ResourceImpl(subject), new PropertyImpl(predicate), new ResourceImpl(object));
    }

    public  void runAnUpdateQuery(String sparqlQuery) {

        try {
            UpdateAction.parseExecute(sparqlQuery, model);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ResultSet runAQuery(String query) {
        try (QueryExecution qExec = QueryExecutionFactory.create(QueryFactory.create(query), model)) {
            ResultSetRewindable results = ResultSetFactory.copyResults(qExec.execSelect());
            qExec.close();
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void write(String file, Lang lang) {
        try {
            RDFDataMgr.write(new FileOutputStream(file), model, Lang.TURTLE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(String file, String lang) {
        try {
            RDFDataMgr.write(new FileOutputStream(file), model, Lang.TURTLE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * NextiaQR methods
     */
    private static GraphOperations instance = null;

    public static GraphOperations getInstance(String jenaPath) {
        if (instance == null) instance = new GraphOperations(jenaPath);
        return instance;
    }

    //Methods to change to other class
    // Short name
    public static String nn(String s) {
        return s.replace(Namespaces.G.val(), "")
                .replace(Namespaces.S.val(), "")
                .replace(Namespaces.sup.val(), "")
                .replace(Namespaces.rdfs.val(), "")
                .replace(Namespaces.owl.val(), "")
                .replace(Namespaces.serginf.val(), "");
    }

    public String convertToURI(String name) {
        //If it is a semantic annotation, add the right URI
        if (name.equals("hasFeature")) {
            return GlobalGraph.HAS_FEATURE.val();
        } else if (name.equals("subClass") || name.equals("subClassOf")) {
            return Namespaces.rdfs.val() + "subClassOf";
        } else if (name.equals("ID") || name.equals("identifier")) {
            return Namespaces.sc.val() + "identifier";
        }

        //Otherwise, just add the SUPERSEDE one
        return Namespaces.sup.val() + name;
    }

    public void addBatchOfTriples(String namedGraph, List<Tuple3<String, String, String>> triples) {
        //System.out.println("Adding triple: [namedGraph] "+namedGraph+", [s] "+s+", [p] "+p+", [o] "+o);
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            for (Tuple3<String, String, String> t : triples) {
                graph.add(new ResourceImpl(t._1), new PropertyImpl(t._2), new ResourceImpl(t._3));
            }
        });
    }

    public void addTriple(String namedGraph, String s, String p, String o) {
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(new ResourceImpl(s), new PropertyImpl(p), new ResourceImpl(o));
        });
    }


    public void registerRDFDataset(String namedGraph, IntegrationGraph I) {
        List<Tuple3<String,String,String>> triples = Lists.newArrayList();

        I.edgeSet().forEach(edge -> {
            CQVertex source = I.getEdgeSource(edge);
            CQVertex target = I.getEdgeTarget(edge);
            if (source.getLabel().contains("Concept") && !source.getLabel().contains("Feature_id"))
                //RDFUtil.addTriple(namedGraph,graphO.convertToURI(source), Namespaces.rdf.val()+"type", GlobalGraph.CONCEPT.val());
                triples.add(new Tuple3<>(convertToURI(source.getLabel()), Namespaces.rdf.val()+"type", GlobalGraph.CONCEPT.val()));
            else if (target.getLabel().contains("Concept") && !target.getLabel().contains("Feature_id"))
                //RDFUtil.addTriple(namedGraph,graphO.convertToURI(source), Namespaces.rdf.val()+"type", GlobalGraph.CONCEPT.val());
                triples.add(new Tuple3<>(convertToURI(source.getLabel()), Namespaces.rdf.val()+"type", GlobalGraph.CONCEPT.val()));
            else if (source.getLabel().contains("Feature"))
                //RDFUtil.addTriple(namedGraph,graphO.convertToURI(source), Namespaces.rdf.val()+"type", GlobalGraph.FEATURE.val());
                triples.add(new Tuple3<>(convertToURI(source.getLabel()), Namespaces.rdf.val()+"type", GlobalGraph.FEATURE.val()));
            else if (target.getLabel().contains("Feature"))
                //RDFUtil.addTriple(namedGraph,graphO.convertToURI(source), Namespaces.rdf.val()+"type", GlobalGraph.FEATURE.val());
                triples.add(new Tuple3<>(convertToURI(source.getLabel()), Namespaces.rdf.val()+"type", GlobalGraph.FEATURE.val()));

            //RDFUtil.addTriple(namedGraph,graphO.convertToURI(source),graphO.convertToURI(edge.getLabel()),graphO.convertToURI(target));
            triples.add(new Tuple3<>(convertToURI(source.getLabel()),convertToURI(edge.getLabel()),convertToURI(target.getLabel())));
        });
//        RDFUtil.addBatchOfTriples(namedGraph,triples);
        addBatchOfTriples(namedGraph,triples);
    }



}
