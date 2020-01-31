package backupCleaner;

import java.nio.file.Paths;
import java.util.concurrent.ForkJoinPool;

public class App {

    public static void main(String[] args) {
    	if(args.length == 0) {
    		System.out.println("Missing parameter basePath");
    		return;
    	}
    	
    	cleanFolder(args[0]);
    }

	public static void cleanFolder(String path) {
		ForkJoinPool commonPool = ForkJoinPool.commonPool();
    	// Start the parallel execution
    	commonPool.invoke(new DirectoryProcessorRecursiveTask(Paths.get(path)));
	}
}
