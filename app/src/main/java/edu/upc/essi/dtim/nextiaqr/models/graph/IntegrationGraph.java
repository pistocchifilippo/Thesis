package edu.upc.essi.dtim.nextiaqr.models.graph;

import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.function.Supplier;

public class IntegrationGraph extends DefaultDirectedGraph<CQVertex, IntegrationEdge> {

    public IntegrationGraph() {
        super(IntegrationEdge.class);
    }

    public IntegrationGraph(Supplier vSupplier, Supplier eSupplier) {
        super(vSupplier,eSupplier,false);
    }

    public void printAsWebGraphViz() {
        System.out.print("digraph \"xx\" {");
        System.out.print("size=\"8,5\"");
        this.edgeSet().forEach(edge -> {
            String source = this.getEdgeSource(edge).getLabel().replace("Concept","C").replace("Feature","F");
            String target = this.getEdgeTarget(edge).getLabel().replace("Concept","C").replace("Feature","F");
            String label = edge.getLabel().replace("hasFeature","hasF");

            System.out.print("\""+source+"\" -> \""+target+"\" [label = \""+label+"\" ];");
        });
        System.out.print("}");
        System.out.println("");
    }
}
