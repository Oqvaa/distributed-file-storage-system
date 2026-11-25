package com.Distributedsystem.DFSS.algorithms;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

@Service
public class ChunkAlgorithm {

    @Value("${chunk.size:1048576}")
    private int chunkSize;

    @Value("${total.nodes:3}")
    private int totalNodes;

    @Value("${storage.nodes.path:./Storage/Node}")
    private String nodesDirectory;

    public static class PartInfo {
        private final int partNumber;
        private final String primaryNode;
        private final String replicaNode;
        private final String primaryPath;
        private final String replicaPath;

        public PartInfo(int partNumber, String primaryNode, String replicaNode,
                        String primaryPath, String replicaPath) {
            this.partNumber = partNumber;
            this.primaryNode = primaryNode;
            this.replicaNode = replicaNode;
            this.primaryPath = primaryPath;
            this.replicaPath = replicaPath;
        }

        public int getPartNumber() { return partNumber; }
        public String getPrimaryNode() { return primaryNode; }
        public String getReplicaNode() { return replicaNode; }
        public String getPrimaryPath() { return primaryPath; }
        public String getReplicaPath() { return replicaPath; }
    }

    public List<PartInfo> splitFile(Path inputFile, int fileId) throws IOException {
        if (totalNodes < 2) {
            throw new IllegalStateException("At least two storage nodes are required for replication.");
        }
        if (chunkSize <= 0) {
            throw new IllegalStateException("chunk.size must be greater than zero.");
        }

        List<PartInfo> parts = new ArrayList<>();
        byte[] buffer = new byte[chunkSize];
        int node = 1;
        int partNumber = 1;
        Path nodeRoot = Path.of(nodesDirectory).toAbsolutePath().normalize();

        try (InputStream in = new BufferedInputStream(Files.newInputStream(inputFile))) {
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                int primaryNode = node;
                int replicaNode = (node % totalNodes) + 1;
                String fileName = fileId + "-part-" + partNumber;

                Path primaryDir = nodeRoot.resolve("Node" + primaryNode);
                Path replicaDir = nodeRoot.resolve("Node" + replicaNode);
                Files.createDirectories(primaryDir);
                Files.createDirectories(replicaDir);

                Path primaryPath = primaryDir.resolve(fileName);
                Path replicaPath = replicaDir.resolve(fileName);

                try {
                    writeChunk(primaryPath, buffer, bytesRead);
                    writeChunk(replicaPath, buffer, bytesRead);
                } catch (IOException | RuntimeException failure) {
                    deleteQuietly(primaryPath, failure);
                    deleteQuietly(replicaPath, failure);
                    throw failure;
                }

                parts.add(new PartInfo(partNumber, "Node" + primaryNode, "Node" + replicaNode,
                        primaryPath.toString(), replicaPath.toString()));

                node = (node % totalNodes) + 1;
                partNumber++;
            }
        } catch (IOException | RuntimeException failure) {
            for (PartInfo part : parts) {
                deleteQuietly(Path.of(part.getPrimaryPath()), failure);
                deleteQuietly(Path.of(part.getReplicaPath()), failure);
            }
            throw failure;
        }

        return parts;
    }

    public List<PartInfo> splitFile(String inputFile, int fileId) throws IOException {
        return splitFile(Path.of(inputFile), fileId);
    }

    private void writeChunk(Path target, byte[] buffer, int length) throws IOException {
        try (BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(
                target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
            out.write(buffer, 0, length);
        }
    }

    private void deleteQuietly(Path path, Throwable originalFailure) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException cleanupFailure) {
            originalFailure.addSuppressed(cleanupFailure);
        }
    }
}
