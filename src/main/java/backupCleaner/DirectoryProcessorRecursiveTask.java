package backupCleaner;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task to process a directory recursively 
 * @author vpete
 *
 */
public class DirectoryProcessorRecursiveTask extends RecursiveTask<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger log = LogManager.getLogger(DirectoryProcessorRecursiveTask.class);

	private Path path;

	public DirectoryProcessorRecursiveTask(Path path) {
		log.debug("DirectoryProcessorRecursiveTask created with "+path.toString());
		this.path = path;
	}

	@Override
	protected Boolean compute() {

		try (Stream<Path> walk = Files.list(path)) {

			// Split the children to files and subdirectorie
			Map<Boolean, List<Path>> directoriesAndFiles = walk.collect(Collectors.partitioningBy(Files::isDirectory));

			// Process the files parallel
			List<Path> files = directoriesAndFiles.get(false);
			boolean everyFilesDeleted = processFileList(files);

			// Process the subdirectories parallel
			List<Path> subDirectories = directoriesAndFiles.get(true);
			boolean everySubDeleted = processDirectoryList(subDirectories);

			if (everyFilesDeleted && everySubDeleted) {
				try {
					log.debug("Deleting folder "+path);
					Files.delete(path);
					return true;

				} catch (DirectoryNotEmptyException ex) {
					// The files.list is weakly consistent, so there can be new files in the folder
					return false;
				}
			}

		} catch (IOException e) {
			log.error("Error during files walk", e);
			return false;
		}

		return false;
	}

	/**
	 * Create one task for each subdirectory, process it parallel then join the
	 * results
	 * 
	 * @param subdirectories
	 * @return
	 */
	private boolean processDirectoryList(List<Path> subdirectories) {
		if (subdirectories == null || subdirectories.isEmpty()) {
			return true;
		}
		return ForkJoinTask.invokeAll(createSubtasksForDirectoryProcessing(subdirectories)).stream()
				.map(ForkJoinTask::join).reduce(Boolean::logicalAnd).isPresent();

	}

	/**
	 * Map each subdirectory to a task
	 * 
	 * @param subDirectories
	 * @return
	 */
	private Collection<DirectoryProcessorRecursiveTask> createSubtasksForDirectoryProcessing(
			List<Path> subDirectories) {
		return subDirectories.stream().map(path -> new DirectoryProcessorRecursiveTask(path))
				.collect(Collectors.toList());
	}

	/**
	 * Process the files parallel, than join the result
	 * 
	 * @param files
	 * @return
	 */
	private boolean processFileList(List<Path> files) {
		if (files == null || files.isEmpty()) {
			return true;
		}
		return ForkJoinTask.invokeAll(createSubtasksForFileProcessing(files)).stream().map(ForkJoinTask::join)
				.reduce(Boolean::logicalAnd).isPresent();

	}

	/**
	 * Create a tasks for the file list, by grouping the files by the first character
	 * This way the files can be processed parallel
	 * 
	 * @param files
	 * @return
	 */
	private Collection<FileListProcessorTask> createSubtasksForFileProcessing(List<Path> files) {
		Map<Character, List<Path>> pathsByStartCharacter = files.stream()
				.collect(Collectors.groupingBy(path -> path.toFile().getName().charAt(0)));

		return pathsByStartCharacter.values().stream().map(list -> new FileListProcessorTask(list))
				.collect(Collectors.toList());
	}

}
