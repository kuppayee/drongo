package in.drongo.drongodb.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {

    private FileUtil() {
        
    }
    
    public static final List<String> getFileNamesStartsWith(File directory, String prefix) {
        final List<String> fileNames = new ArrayList<>();
        File[] files = directory.listFiles();
        for (File file : files) {
            if(file.getName().startsWith(prefix)) {
                fileNames.add(file.getName());
            }
        }
        return fileNames;
    }
    
    public static final List<Long> getFileNamesStartsWith(File directory, String prefix, long curFile) {
        final List<Long> fileNames = new ArrayList<>();
        File[] files = directory.listFiles();
        for (File file : files) {
            if(file.getName().startsWith(prefix)) {
                long age = Long.parseLong(file.getName().substring(8));
                if (age <= curFile) {
                    fileNames.add(age);
                }
            }
        }
        return fileNames;
    }

}
