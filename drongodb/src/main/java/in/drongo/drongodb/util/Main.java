package in.drongo.drongodb.util;

import java.io.File;

import in.drongo.drongodb.DrongoDB;
import in.drongo.drongodb.DrongoDBOptions;
import in.drongo.drongodb.internal.DefaultDrongoDB;

public class Main {
    
    public static void main(String[] args) throws Exception {
        
        DrongoDB drongoDB = new DefaultDrongoDB().open(new File("C:/demo"), new DrongoDBOptions());
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                drongoDB.put((i + "sanju").getBytes(), "ajith boss ele jhon suriya sutti".getBytes());
                drongoDB.put((i + "seetha").getBytes(), "sutti suriya jhon ele boss ajith".getBytes());
            }
            System.out.println(System.currentTimeMillis() - start);
        } finally {
            drongoDB.close();
        }
        System.out.println("main");
    
    }
    
}
