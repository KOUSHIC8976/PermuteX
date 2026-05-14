resource "aws_msk_cluster" "permutex_kafka" {
  cluster_name           = "permutex-telemetry-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.m5.large"
    # subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id, aws_subnet.private_3.id]
    # security_groups = [aws_security_group.kafka_sg.id]
    
    storage_info {
      ebs_storage_info {
        volume_size = 1000 # 1TB per broker for high-throughput telemetry
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  tags = {
    Engine = "Kafka"
  }
}