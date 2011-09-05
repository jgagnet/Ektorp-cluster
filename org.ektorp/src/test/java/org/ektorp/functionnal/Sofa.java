package org.ektorp.functionnal;


import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonWriteNullProperties;

@JsonWriteNullProperties(false)
@JsonIgnoreProperties({"id", "revision"})
public class Sofa {

    @JsonProperty("_id")
    private String id;

    @JsonProperty("_rev")
    private String revision;

    private String color;

    public Sofa(String color){
        this.color = color;
    }

    public void setId(String s) {
        id = s;
    }

    public String getId() {
        return id;
    }

    public String getRevision() {
        return revision;
    }

    public void setColor(String s) {
        color = s;
    }

    public String getColor() {
        return color;
    }
}
