package com.Distributedsystem.DFSS.entity;

import com.Distributedsystem.DFSS.repository.StorageNodeRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class StorageNodeInitializer implements CommandLineRunner {

    private final StorageNodeRepository storageNodeRepository;

    @Value("${total.nodes:3}")
    private int totalNodes;

    public StorageNodeInitializer(StorageNodeRepository storageNodeRepository) {
        this.storageNodeRepository = storageNodeRepository;
    }

    @Override
    public void run(String... args) {
        if (totalNodes < 2) {
            throw new IllegalStateException("At least two storage nodes are required for replication.");
        }

        for (int index = 1; index <= totalNodes; index++) {
            String nodeId = "Node" + index;
            if (storageNodeRepository.existsById(nodeId)) {
                continue;
            }

            StorageNode node = new StorageNode();
            node.setNodeId(nodeId);
            node.setNodeName("Storage Node " + index);
            node.setTotalStorage(10L * 1024 * 1024 * 1024);
            node.setUsedStorage(0);
            node.setOnline(true);
            storageNodeRepository.save(node);
        }
    }
}
