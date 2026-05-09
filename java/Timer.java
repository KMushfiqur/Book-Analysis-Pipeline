// Timer.java
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Timer {
    private long startTime;
    private long endTime;

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        endTime = System.currentTimeMillis();
    }

    public long getElapsedTimeMillis() {
        return endTime - startTime;
    }

    public double getElapsedTimeSeconds() {
        return (endTime - startTime) / 1000.0;
    }

    // Write elapsed time to a file
    public void writeElapsedTime(String taskName, String filePath) {
        try (PrintWriter out = new PrintWriter(new FileWriter(filePath, true))) {
            out.println(taskName + " took " + getElapsedTimeMillis() + " ms (" +
                        getElapsedTimeSeconds() + " s).");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
