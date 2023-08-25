package pt.uc.is.dto;

import java.io.Serializable;

public class WeatherStandard implements Serializable {
    Long Id;
    String Location;
    Long temperature;

    public WeatherStandard() {}
    public WeatherStandard(Long id, String location, Long temperature) {
        Id = id;
        Location = location;
        this.temperature = temperature;
    }

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public String getLocation() {
        return Location;
    }

    public void setLocation(String location) {
        Location = location;
    }

    public Long getTemperature() {
        return temperature;
    }

    public void setTemperature(Long temperature) {
        this.temperature = temperature;
    }
}
