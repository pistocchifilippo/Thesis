package edu.upc.essi.dtim.nextiaqr.models.querying;

import java.util.List;

public class RelationalSchema {

    List<String> attributes;

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "RelationalSchema{" +
                "attributes=" + attributes +
                '}';
    }
}
