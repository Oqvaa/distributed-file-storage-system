package com.Distributedsystem.DFSS.algorithms;

import com.Distributedsystem.DFSS.entity.FileEntity;
import com.Distributedsystem.DFSS.entity.FilePart;
import com.Distributedsystem.DFSS.repository.FilePartRepository;
import com.Distributedsystem.DFSS.repository.FileRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Service
public class FileService {

    private final FileRepository fileRepository;
    private final FilePartRepository filePartRepository;

    public FileService(FileRepository fileRepository, FilePartRepository filePartRepository) {
        this.fileRepository = fileRepository;
        this.filePartRepository = filePartRepository;
    }

    @Transactional
    public void deleteFile(Integer fileId) throws IOException {
        FileEntity file = fileRepository.findById(fileId)
                .orElseThrow(() -> new IllegalArgumentException("File not found: " + fileId));

        List<FilePart> parts = filePartRepository.findByFileId(String.valueOf(fileId));

        // Remove metadata only after all physical copies have been removed successfully.
        for (FilePart part : parts) {
            Path path = Path.of(part.getPartPath()).toAbsolutePath().normalize();
            Files.deleteIfExists(path);
        }

        filePartRepository.deleteAllInBatch(parts);
        fileRepository.delete(file);
    }
}
