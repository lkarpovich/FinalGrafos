package ar.edu.itba.graph;

import java.io.Serializable;

public class Edge implements Serializable {
    private Long id;
    private Long src;
    private Long des;
    private String labelE;
    private Integer dist;

    public Edge() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getSrc() {
        return src;
    }

    public void setSrc(Long src) {
        this.src = src;
    }

    public Long getDes() {
        return des;
    }

    public void setDes(Long des) {
        this.des = des;
    }

    public String getLabelE() {
        return labelE;
    }

    public void setLabelE(String labelE) {
        this.labelE = labelE;
    }

    public Integer getDist() {
        return dist;
    }

    public void setDist(Integer dist) {
        this.dist = dist;
    }
}
