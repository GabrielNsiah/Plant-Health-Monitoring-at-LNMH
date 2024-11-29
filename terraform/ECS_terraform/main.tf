provider "aws" {
  region = "eu-west-2"
}

data "aws_vpc" "c14-vpc" {
    id = var.C14_VPC
}

data "aws_subnet" "c14-subnet-1" {
  id = var.C14_SUBNET_1
}

data "aws_subnet" "c14-subnet-2" {
  id = var.C14_SUBNET_2
}

data "aws_subnet" "c14-subnet-3" {
  id = var.C14_SUBNET_3
}


data "aws_ecs_cluster" "c14-cluster" {
    cluster_name = var.C14_CLUSTER
}

data "aws_iam_role" "execution-role" {
    name = "ecsTaskExecutionRole"
}

resource "aws_ecs_task_definition" "c14-gbu-plants-dashboard-ECS-task-def-tf" {
  family                   = "c14-gbu-plants-dashboard-ECS-task-def-tf"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  execution_role_arn       = data.aws_iam_role.execution-role.arn
  cpu                      = 1024
  memory                   = 2048
  container_definitions    = jsonencode([
    {
      name         = "c14-gbu-plants-dashboard-ECS-task-def-tf"
      image        = var.URI
      cpu          = 10
      memory       = 512
      essential    = true
      portMappings = [
        {
            containerPort = 80
            hostPort      = 80
        },
        {
            containerPort = 8501
            hostPort      = 8501       
        }
      ]
      environment= [
                {
                    "name": "aws_access_key_id",
                    "value": var.aws_access_key_id
                },
                {
                    "name": "aws_secret_access_key",
                    "value": var.aws_secret_access_key
                },
                {
                    "name": "DB_NAME",
                    "value": var.DB_NAME
                },
                {
                    "name": "DB_USER",
                    "value": var.DB_USER
                },
                {
                    "name": "DB_PASSWORD",
                    "value": var.DB_PASSWORD
                },
                {
                    "name": "DB_HOST",
                    "value": var.DB_HOST
                },
                {
                    "name": "DB_PORT",
                    "value": var.DB_PORT
                },
                {
                    "name": "SCHEMA_NAME",
                    "value": var.SCHEMA_NAME
                }
            ]
            logConfiguration = {
                logDriver = "awslogs"
                options = {
                    "awslogs-create-group"  = "true"
                    "awslogs-group"         = "/ecs/c14-gbu-plants-ECS-task-def-tf"
                    "awslogs-region"        = "eu-west-2"
                    "awslogs-stream-prefix" = "ecs"
                }
            }
    },
  ])
}

resource "aws_security_group" "c14-gbu-plants-dashboard-sg-tf" {
    name        = "c14-gbu-plants-dashboard-sg-tf"
    description = "Security group for connecting to dashboard"
    vpc_id      = data.aws_vpc.c14-vpc.id

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
    ingress {
        from_port   = 8501
        to_port     = 8501
        protocol    = "tcp"
        cidr_blocks = ["0.0.0.0/0"]
    }
}

resource "aws_ecs_service" "c14-gbu-plants-dashboard-service-tf" {
    name            = "c14-gbu-plants-dashboard-service-tf"
    cluster         = data.aws_ecs_cluster.c14-cluster.id
    task_definition = aws_ecs_task_definition.c14-gbu-plants-dashboard-ECS-task-def-tf.arn
    desired_count   = 1
    launch_type     = "FARGATE" 
    
    network_configuration {
        subnets          = [data.aws_subnet.c14-subnet-1.id, data.aws_subnet.c14-subnet-2.id, data.aws_subnet.c14-subnet-3.id] 
        security_groups  = [aws_security_group.c14-gbu-plants-dashboard-sg-tf.id] 
        assign_public_ip = true
    }
}
