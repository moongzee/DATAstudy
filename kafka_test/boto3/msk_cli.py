import boto3


client = boto3.client('kafka')

response = client.create_cluster(
                                    BrokerNodeGroupInfo={
                                        'BrokerAZDistribution': 'DEFAULT',
                                        'ClientSubnets': [
                                            'subnet-XXX',
                                            'subnet-XXX',
                                            'subnet-XXX'
                                        ],
                                        'InstanceType': 'string',
                                        'SecurityGroups': [
                                            'sg-XXX',
                                            'sg-XXX'
                                        ],
                                        'StorageInfo': {
                                            'EbsStorageInfo': {
                                                'ProvisionedThroughput': {
                                                    'Enabled': False,
                                                },
                                                'VolumeSize': 500
                                            }
                                        },
                                        'ConnectivityInfo': {
                                            'PublicAccess': {
                                                'Type': 'DISABLED'
                                            }
                                        }
                                                        },
                                    ClientAuthentication={
                                        'Sasl': {
                                            'Scram': {
                                                'Enabled': False
                                            },
                                            'Iam': {
                                                'Enabled': True
                                            }
                                        },
                                        'Tls': {
                                            'CertificateAuthorityArnList': [
                                                'string',
                                            ],
                                            'Enabled': False
                                        },
                                        'Unauthenticated': {
                                            'Enabled': True
                                        }
                                                        },
                                    ClusterName='cluster_name',
                                    ConfigurationInfo={
                                        'Arn': 'arn:aws:kafka:XXX',
                                        'Revision': 4
                                    },
                                    EncryptionInfo={
                                        'EncryptionAtRest': {
                                            'DataVolumeKMSKeyId': 'arn:aws:kms:XXX'
                                        },
                                        'EncryptionInTransit': {
                                            'ClientBroker': 'TLS_PLAINTEXT',
                                            'InCluster': True
                                        }
                                    },
                                    EnhancedMonitoring='PER_TOPIC_PER_BROKER',
                                    OpenMonitoring={
                                        'Prometheus': {
                                            'JmxExporter': {
                                                'EnabledInBroker': False
                                            },
                                            'NodeExporter': {
                                                'EnabledInBroker': False
                                            }
                                        }
                                    },
                                    KafkaVersion='2.6.2',
                                    LoggingInfo={
                                        'BrokerLogs': {
                                            'CloudWatchLogs': {
                                                'Enabled': True,
                                                'LogGroup': 'msk-oasis-prd'
                                            },
                                            'Firehose': {
                                                'Enabled': False
                                            },
                                            'S3': {
                                                'Enabled': False,
                                            }
                                        }
                                    },
                                    NumberOfBrokerNodes=3,
                                    Tags={
                                    }
                                )                                                             