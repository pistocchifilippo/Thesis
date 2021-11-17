package edu.upc.essi.dtim.nextiaqr.models.metamodel;

/**
 * Created by snadal on 15/12/16.
 */
public class Metamodel {

    /**
     * Method to check if a given IRI is part of the metamodel of the type
     */
    public static boolean contains(String type, String elem) {
        if (type.equals("SOURCE")) {
            for (SourceGraph s : SourceGraph.values()) {
                if (s.val().equals(elem)) return true;
            }
            return false;
        }
        if (type.equals("GLOBAL")) {
            for (GlobalGraph g : GlobalGraph.values()) {
                if (g.val().equals(elem)) return true;
            }
            return false;
        }
        /*if (type.equals("BDI_ONTOLOGY")) {
            for (GlobalLevel g : GlobalLevel.values()) {
                if (g.val().equals(elem)) return true;
            }
            for (SourceLevel g : SourceLevel.values()) {
                if (g.val().equals(elem)) return true;
            }
            for (Mappings g : Mappings.values()) {
                if (g.val().equals(elem)) return true;
            }
            return false;
        }*/
        return false;
    }

}
