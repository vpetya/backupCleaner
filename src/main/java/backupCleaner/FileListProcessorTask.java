package backupCleaner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task to process a file list, deleting the .bak files without an original
 * @author vpete
 *
 */
public class FileListProcessorTask extends RecursiveTask<Boolean> {

	private static Logger log = LogManager.getLogger(FileListProcessorTask.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	List<Path> files;

	public FileListProcessorTask(List<Path> files) {
		log.debug("FileListProcessorTask created with "+files.size());
		this.files = files;
	}

	@Override
	protected Boolean compute() {

		// Split the children to bak and not bak
		Map<Boolean, List<Path>> bakAndOriginalFiles = files.stream().collect(Collectors.partitioningBy(file -> {
			String name = file.toString();
			return name.length() > 4 && name.endsWith(".bak");
		}));

		// Put every original file name in a set, for quick access
		final Set<String> originalNames = bakAndOriginalFiles.get(false).stream()
				.map(FileListProcessorTask::getFileNameWithoutExtension).collect(Collectors.toSet());

		boolean allBakFilesDeleted = bakAndOriginalFiles.get(true).stream().map(path -> {
			if (!originalNames.contains(FileListProcessorTask.getFileNameWithoutExtension(path))) {
				try {
					log.debug("Deleting bak file "+path);
					Files.delete(path);
					return true;
				} catch (IOException e) {
					log.error("Error deleting bak file", e);
				}
			}
			return false;
		}).reduce(Boolean::logicalAnd).isPresent();

		// No original files and no remaining bakFiles
		return bakAndOriginalFiles.get(false).isEmpty() && allBakFilesDeleted;
	}

	public static String getFileNameWithoutExtension(Path path) {
		String fileName = path.toFile().getName();

		if (fileName.indexOf(".") > 0) {
			return fileName.substring(0, fileName.lastIndexOf("."));
		} else {
			return fileName;
		}
	}
}
