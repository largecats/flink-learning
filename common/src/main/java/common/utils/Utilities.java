package common.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Files;

public class Utilities {

    public static Boolean removeDir(String path) throws Exception {
        File pathToRemove = new File(path);
        if (pathToRemove.exists()) {
            FileUtils.cleanDirectory(pathToRemove);
            FileUtils.deleteDirectory(pathToRemove);
            return true;
        } else {
            return false;
        }
    }
}
