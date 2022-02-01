package ar.edu.itba.graph;

import java.io.Serializable;

public class Vertex implements Serializable {
    private Long id;
    private String type;
    private String code;
    private String icao;
    private String desc;
    private String region;
    private String country;
    private String city;
    private String author;
    private String date;
    private String labelV;
    private Integer runways;
    private Integer longest;
    private Integer elev;
    private Double lat;
    private Double lon;

    public Vertex() {
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public void setIcao(String icao) {
        this.icao = icao;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setLabelV(String labelV) {
        this.labelV = labelV;
    }

    public void setRunways(Integer runways) {
        this.runways = runways;
    }

    public void setLongest(Integer longest) {
        this.longest = longest;
    }

    public void setElev(Integer elev) {
        this.elev = elev;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public Long getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getCode() {
        return code;
    }

    public String getIcao() {
        return icao;
    }

    public String getDesc() {
        return desc;
    }

    public String getRegion() {
        return region;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public String getAuthor() {
        return author;
    }

    public String getDate() {
        return date;
    }

    public String getLabelV() {
        return labelV;
    }

    public Integer getRunways() {
        return runways;
    }

    public Integer getLongest() {
        return longest;
    }

    public Integer getElev() {
        return elev;
    }

    public Double getLat() {
        return lat;
    }

    public Double getLon() {
        return lon;
    }
}
