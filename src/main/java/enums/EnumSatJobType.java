package main.java.enums;

/**
 * Created by marbarfa on 2/12/14.
 */
public enum EnumSatJobType {

    initial_configuration("init"),
    iteration("rec");


    private String type = null;

    EnumSatJobType(String inputType){
        this.type = inputType;
    }

    public String getType() {
        return type;
    }
}
