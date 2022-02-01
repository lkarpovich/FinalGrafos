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

    public Vertex(Long id, String type, String code, String icao, String desc, String region, String country,
                  String city, String author, String date, String labelV, String runways, String longest,
                  String elev, String lat, String lon) {
        this.id = id;
        this.type = type;
        this.code = code;
        this.icao = icao;
        this.desc = desc;
        this.region = region;
        this.country = country;
        this.city = city;
        this.author = author;
        this.date = date;
        this.labelV = labelV;
        this.runways = Integer.valueOf(runways);
        this.longest = Integer.valueOf(longest);
        this.elev = Integer.valueOf(elev);
        this.lat = Double.valueOf(lat);
        this.lon = Double.valueOf(lon);
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
