package pt.uc.is.dto;

import java.io.Serializable;

public class WeatherAlert implements Serializable {
    String type;
    String location;
    String eventColor;

    public WeatherAlert() {}
    public WeatherAlert(String type, String location, String eventColor) {
        this.type = type;
        this.location = location;
        this.eventColor = eventColor;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getEventColor() {
        return eventColor;
    }

    public void setEventColor(String eventColor) {
        this.eventColor = eventColor;
    }
}
