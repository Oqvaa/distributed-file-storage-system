package com.Distributedsystem.DFSS.algorithms;

import com.Distributedsystem.DFSS.entity.FilePart;
import com.Distributedsystem.DFSS.repository.FilePartRepository;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class MergeAlgorithm {

    private final FilePartRepository filePartRepository;

    public MergeAlgorithm(FilePartRepository filePartRepository) {
        this.filePartRepository = filePartRepository;
    }

    /**
     * Stream a reconstructed file to the caller. The servlet output stream remains owned by
     * Spring, so this method flushes but does not close it.
     */
    public void writeTo(int totalParts, int fileId, OutputStream target) throws IOException {
        Map<Integer, List<FilePart>> partsByNumber = loadParts(fileId);
        byte[] copyBuffer = new byte[64 * 1024];
        OutputStream out = new BufferedOutputStream(target);

        for (int partNumber = 1; partNumber <= totalParts; partNumber++) {
            List<FilePart> copies = partsByNumber.get(partNumber);
            if (copies == null || copies.isEmpty()) {
                throw new IOException("Metadata missing for chunk " + partNumber
                        + " of file " + fileId);
            }

            IOException lastFailure = null;
            byte[] readableChunk = null;
            for (FilePart copy : copies.stream()
                    .sorted(Comparator.comparing(FilePart::getNodeId))
                    .toList()) {
                Path chunkPath = Path.of(copy.getPartPath()).toAbsolutePath().normalize();
                try (InputStream in = new BufferedInputStream(Files.newInputStream(chunkPath));
                     ByteArrayOutputStream chunkBuffer = new ByteArrayOutputStream()) {
                    int read;
                    while ((read = in.read(copyBuffer)) != -1) {
                        chunkBuffer.write(copyBuffer, 0, read);
                    }
                    // Buffer each chunk before writing so a mid-read failure cannot corrupt
                    // the response before the replica is attempted.
                    readableChunk = chunkBuffer.toByteArray();
                    break;
                } catch (IOException exception) {
                    lastFailure = exception;
                }
            }

            if (readableChunk == null) {
                throw new IOException("No readable replica for chunk " + partNumber
                        + " of file " + fileId, lastFailure);
            }
            // Keep response-write errors separate from source-read errors. A disconnected
            // client must not trigger a pointless retry against another replica.
            out.write(readableChunk);
        }
        out.flush();
    }

    /**
     * Disk-based compatibility method useful for tests and administrative exports.
     * The final target is replaced only after every chunk has been reconstructed.
     */
    public void mergeFiles(int totalParts, String outputFilePath, int fileId) throws IOException {
        Path output = Path.of(outputFilePath).toAbsolutePath().normalize();
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }
        Path temporaryOutput = Files.createTempFile(output.getParent(), "dfss-", ".assembling");
        boolean completed = false;
        try {
            try (OutputStream out = Files.newOutputStream(temporaryOutput,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                writeTo(totalParts, fileId, out);
            }
            try {
                Files.move(temporaryOutput, output, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException exception) {
                Files.move(temporaryOutput, output, StandardCopyOption.REPLACE_EXISTING);
            }
            completed = true;
        } finally {
            if (!completed) {
                Files.deleteIfExists(temporaryOutput);
            }
        }
    }

    private Map<Integer, List<FilePart>> loadParts(int fileId) {
        return filePartRepository.findByFileIdOrderByPartNumberAsc(String.valueOf(fileId))
                .stream()
                .collect(Collectors.groupingBy(FilePart::getPartNumber));
    }
}
