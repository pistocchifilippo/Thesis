package edu.upc.essi.dtim.nextiaqr.models.querying;

import com.google.common.collect.Maps;
import edu.upc.essi.dtim.nextiaqr.models.metamodel.Namespaces;

import java.util.EnumSet;
import java.util.Map;

public class EquiJoin extends RelationalOperator {

    private String left_attribute;
    private String right_attribute;

    public EquiJoin() {}

    public EquiJoin (String left, String right) {
        this.left_attribute = left;
        this.right_attribute = right;
    }

    public String getLeft_attribute() {
        return left_attribute;
    }

    public void setLeft_attribute(String left_attribute) {
        this.left_attribute = left_attribute;
    }

    public String getRight_attribute() {
        return right_attribute;
    }

    public void setRight_attribute(String right_attribute) {
        this.right_attribute = right_attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof EquiJoin) {
            final EquiJoin other = (EquiJoin)o;
            return (left_attribute.equals(other.getLeft_attribute()) && right_attribute.equals(other.getRight_attribute())) ||
                   (left_attribute.equals(other.getRight_attribute()) && right_attribute.equals(other.getLeft_attribute()));
            /*return (Objects.equals(left_attribute,other.left_attribute) &&
                    Objects.equals(right_attribute, other.right_attribute)) ||
                    (Objects.equals(left_attribute,other.right_attribute) &&
                    (Objects.equals(right_attribute,other.left_attribute)));*/
        } else {
            return false;
        }
    }


    @Override
    public int hashCode() {
        //https://stackoverflow.com/questions/113511/best-implementation-for-hashcode-method-for-a-collection
        return 37 * (left_attribute.hashCode()+206) + 37 * (right_attribute.hashCode()+206);
/*        if (left_attribute.compareTo(right_attribute) == -1 || left_attribute.compareTo(right_attribute) == 0)
            return Objects.hash(left_attribute,right_attribute);
        else return Objects.hash(right_attribute,left_attribute);*/
    }


    @Override
    public String toString() {

        Map<String,String> namespaces = Maps.newHashMap();
        EnumSet.allOf(Namespaces.class).forEach(e -> namespaces.put(e.val(),e.name()));

        String uri_left = namespaces.keySet().stream().filter(n -> left_attribute.contains(n)).findFirst().get();
        String uri_right = namespaces.keySet().stream().filter(n -> right_attribute.contains(n)).findFirst().get();

        return left_attribute.replace(uri_left,namespaces.get(uri_left)+":") + "=" +
                right_attribute.replace(uri_right,namespaces.get(uri_right)+":")+" ";
    }
}
