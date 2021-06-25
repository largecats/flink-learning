package appParams;

import org.apache.flink.api.java.utils.ParameterTool;

public class ReadParamsFromFile {

    public static void main(String[] args) throws Exception {
        String propertiesFilePath = "./examples/src/main/java/appParams/application.properties"; // can start the path from project root
        ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);
        System.out.println(parameter.get("parallelism"));
    }
}
