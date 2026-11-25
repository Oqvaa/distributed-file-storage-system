package com.Distributedsystem.DFSS.controller;

import com.Distributedsystem.DFSS.algorithms.ChunkAlgorithm;
import com.Distributedsystem.DFSS.algorithms.FileService;
import com.Distributedsystem.DFSS.algorithms.MergeAlgorithm;
import com.Distributedsystem.DFSS.entity.FileEntity;
import com.Distributedsystem.DFSS.entity.FilePart;
import com.Distributedsystem.DFSS.entity.StorageNode;
import com.Distributedsystem.DFSS.repository.FilePartRepository;
import com.Distributedsystem.DFSS.repository.FileRepository;
import com.Distributedsystem.DFSS.repository.StorageNodeRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ContentDisposition;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/files")
public class FileController {

    private final ChunkAlgorithm chunkAlgorithm;
    private final FilePartRepository filePartRepository;
    private final MergeAlgorithm mergeAlgorithm;
    private final FileRepository fileRepository;
    private final FileService fileService;
    private final StorageNodeRepository storageNodeRepository;
    private final Path outputDirectory;

    public FileController(ChunkAlgorithm chunkAlgorithm,
                          FilePartRepository filePartRepository,
                          MergeAlgorithm mergeAlgorithm,
                          FileRepository fileRepository,
                          FileService fileService,
                          StorageNodeRepository storageNodeRepository,
                          @Value("${storage.path}") String storageDirectory) {
        this.chunkAlgorithm = chunkAlgorithm;
        this.filePartRepository = filePartRepository;
        this.mergeAlgorithm = mergeAlgorithm;
        this.fileRepository = fileRepository;
        this.fileService = fileService;
        this.storageNodeRepository = storageNodeRepository;
        this.outputDirectory = Paths.get(storageDirectory).toAbsolutePath().normalize();
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        if (file == null || file.isEmpty()) {
            return ResponseEntity.badRequest().body("Please select a non-empty file to upload.");
        }

        Path temporaryUpload = null;
        FileEntity savedFile = null;
        List<ChunkAlgorithm.PartInfo> createdParts = List.of();

        try {
            Files.createDirectories(outputDirectory);
            String originalFileName = sanitizeFileName(file.getOriginalFilename());
            temporaryUpload = Files.createTempFile(outputDirectory, "dfss-upload-", ".tmp");
            file.transferTo(temporaryUpload);

            FileEntity fileEntity = new FileEntity();
            fileEntity.setFileName(originalFileName);
            fileEntity.setFileSize(file.getSize());
            fileEntity.setTotalParts(0);
            fileEntity.setUploadedAt(LocalDateTime.now());

            // The database sequence gives each upload a unique ID, including concurrent uploads.
            savedFile = fileRepository.saveAndFlush(fileEntity);
            int fileId = savedFile.getId();

            createdParts = chunkAlgorithm.splitFile(temporaryUpload, fileId);

            List<FilePart> records = new ArrayList<>(createdParts.size() * 2);
            for (ChunkAlgorithm.PartInfo part : createdParts) {
                records.add(toFilePart(fileId, part.getPartNumber(), part.getPrimaryNode(),
                        part.getPrimaryPath()));
                records.add(toFilePart(fileId, part.getPartNumber(), part.getReplicaNode(),
                        part.getReplicaPath()));
            }

            filePartRepository.saveAll(records);
            savedFile.setTotalParts(createdParts.size());
            fileRepository.save(savedFile);

            return ResponseEntity.ok("File uploaded successfully: " + originalFileName
                    + " (id=" + fileId + ", chunks=" + createdParts.size() + ")");
        } catch (IOException | RuntimeException exception) {
            cleanupUpload(temporaryUpload, createdParts, savedFile);
            return ResponseEntity.internalServerError()
                    .body("Failed to upload file: " + exception.getMessage());
        } finally {
            if (temporaryUpload != null) {
                deleteQuietly(temporaryUpload);
            }
        }
    }

    private FilePart toFilePart(int fileId, int partNumber, String nodeId, String partPath) {
        FilePart part = new FilePart();
        part.setId(java.util.UUID.randomUUID().toString());
        part.setFileId(String.valueOf(fileId));
        part.setPartNumber(partNumber);
        part.setNodeId(nodeId);
        part.setPartPath(partPath);
        return part;
    }

    private void cleanupUpload(Path temporaryUpload,
                               List<ChunkAlgorithm.PartInfo> createdParts,
                               FileEntity savedFile) {
        for (ChunkAlgorithm.PartInfo part : createdParts) {
            deleteQuietly(Path.of(part.getPrimaryPath()));
            deleteQuietly(Path.of(part.getReplicaPath()));
        }

        if (savedFile != null && savedFile.getId() != null) {
            try {
                filePartRepository.deleteByFileId(String.valueOf(savedFile.getId()));
                fileRepository.deleteById(savedFile.getId());
            } catch (RuntimeException ignored) {
                // Preserve the original failure; orphan cleanup can be retried operationally.
            }
        }
        if (temporaryUpload != null) {
            deleteQuietly(temporaryUpload);
        }
    }

    private void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Best-effort cleanup.
        }
    }

    private String sanitizeFileName(String suppliedName) {
        if (suppliedName == null || suppliedName.isBlank()) {
            return "uploaded-file";
        }
        String normalized = suppliedName.replace('\\', '/');
        Path suppliedPath = Path.of(normalized);
        Path fileNamePath = suppliedPath.getFileName();
        if (fileNamePath == null) {
            return "uploaded-file";
        }
        String fileName = fileNamePath.toString();
        if (fileName.isBlank() || fileName.equals(".") || fileName.equals("..")) {
            return "uploaded-file";
        }
        return fileName;
    }

    @GetMapping("/download/{fileId}")
    public ResponseEntity<StreamingResponseBody> downloadFile(@PathVariable Integer fileId) {
        FileEntity file = fileRepository.findById(fileId).orElse(null);
        if (file == null) {
            return ResponseEntity.notFound().build();
        }

        StreamingResponseBody responseBody = outputStream ->
                mergeAlgorithm.writeTo(file.getTotalParts(), fileId, outputStream);
        MediaType contentType = MediaTypeFactory.getMediaType(file.getFileName())
                .orElse(MediaType.APPLICATION_OCTET_STREAM);

        return ResponseEntity.ok()
                .contentType(contentType)
                .contentLength(file.getFileSize())
                .header("Content-Disposition", ContentDisposition.attachment()
                        .filename(file.getFileName(), StandardCharsets.UTF_8).build().toString())
                .body(responseBody);
    }

    @GetMapping
    public List<FileEntity> getAllFiles() {
        return fileRepository.findAll();
    }

    @DeleteMapping("/{fileId}")
    public ResponseEntity<String> deleteFile(@PathVariable Integer fileId) {
        try {
            fileService.deleteFile(fileId);
            return ResponseEntity.ok("File deleted successfully.");
        } catch (IllegalArgumentException exception) {
            return ResponseEntity.notFound().build();
        } catch (IOException exception) {
            return ResponseEntity.internalServerError()
                    .body("Failed to delete file: " + exception.getMessage());
        }
    }

    @GetMapping("/{id}")
    public ResponseEntity<FileEntity> getFileById(@PathVariable Integer id) {
        return fileRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/nodes")
    public List<StorageNode> getAllNodes() {
        return storageNodeRepository.findAll();
    }
}
