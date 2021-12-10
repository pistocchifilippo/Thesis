package edu.upc.essi.dtim.nextiaqr.models.metamodel;

/**
 * Created by snadal on 6/06/17.
 */
public enum SourceGraph {

    DATA_SOURCE(Namespaces.S.val()+"DataSource"),
    WRAPPER(Namespaces.S.val()+"Wrapper"),
    ATTRIBUTE(Namespaces.S.val()+"Attribute"),

    HAS_WRAPPER(Namespaces.S.val()+"hasWrapper"),
    HAS_ATTRIBUTE(Namespaces.S.val()+"hasAttribute");


    private String element;

    SourceGraph(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }
}
