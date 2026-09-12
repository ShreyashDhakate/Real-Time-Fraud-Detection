terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_security_group" "fraud" {
  name_prefix = "fraud-demo-"
  description = "No inbound access; administer through SSM"
  vpc_id      = var.vpc_id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "instance" {
  name_prefix = "fraud-demo-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "fraud" {
  name_prefix = "fraud-demo-"
  role        = aws_iam_role.instance.name
}

resource "aws_instance" "fraud" {
  ami                         = var.ubuntu_ami_id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  associate_public_ip_address = false
  vpc_security_group_ids      = [aws_security_group.fraud.id]
  iam_instance_profile        = aws_iam_instance_profile.fraud.name
  metadata_options {
    http_tokens = "required"
  }
  root_block_device {
    volume_size           = 80
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = false
  }
  user_data = <<-EOF
    #!/bin/bash
    set -euo pipefail
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io docker-compose-v2 git
    systemctl enable --now docker
    mkdir -p /opt/fraud
    chown ubuntu:ubuntu /opt/fraud
  EOF
  tags = { Name = "fraud-pipeline-demo" }
}

output "instance_id" {
  value = aws_instance.fraud.id
}
