# Things to set up outside the repository

I can run the in-process demo and unit tests without any cloud accounts. I need Docker for the full local stream, Kaggle access for the real training dataset, and AWS only when I am ready to deploy.

## 1. Docker Desktop (installed locally)

Docker Desktop is now installed for this Windows user at `%LOCALAPPDATA%\Programs\DockerDesktop`, using the existing WSL 2 setup. Docker Engine 29.7.2 and Compose v5.5.1 were verified. Open a fresh terminal so its PATH includes the new installation.

For a fresh workstation, install [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/) and follow its WSL 2 prerequisites. Restart if requested, launch Docker Desktop, and select Linux containers.

Verify in a new PowerShell window:

```powershell
docker version
docker compose version
```

Allow enough Docker memory for Kafka, four Python workers, Redis, and the scorer. Start with at least 8 GB available to Docker and reduce competing workloads; this is a development starting point, not a measured capacity requirement. Docker needs outbound access to download images and packages.

Then use the full-stack commands in the README. Kafka, Redis, and the scoring endpoint bind to localhost on the host. No Confluent account, paid Redis service, or external API key is needed.

## 2. IEEE-CIS training data (available locally)

The dataset is now available in `../ieee-fraud-detection/`. The initial model has been trained from that location and saved under `artifacts/model/`. Its held-out ROC-AUC is 0.6723; see [the baseline report](../reports/IEEE_BASELINE.md). There is no need to copy the large CSV files into this repository.

For a fresh workstation, use these acquisition steps:

1. Sign in to Kaggle and open the [IEEE-CIS Fraud Detection data page](https://www.kaggle.com/c/ieee-fraud-detection/data?select=train_transaction.csv).
2. Accept any dataset/competition access terms presented by Kaggle.
3. Download and extract `train_transaction.csv` to `data/ieee/train_transaction.csv`.
4. Run `fraud train --input data/ieee/train_transaction.csv --output artifacts/model` from the project's virtual environment.

The adapter requires `TransactionID`, `TransactionDT`, `TransactionAmt`, and `isFraud`. It uses `card1`, `card2`, `card3`, `card5`, and `addr1` when present to construct an anonymized entity proxy. The identity file is not used in this version.

The source's relative timestamps are for chronological training. Do not replay them directly into the live scoring service, which checks UTC snapshot age. The synthetic producer uses recent UTC timestamps for the demo.

Inspect `artifacts/model/evaluation.json`; use the reported held-out result in any project description. Raw data and model artifacts are ignored by Git.

## 3. Run k6 through Docker

The project now includes the official k6 Docker image as an optional Compose service. A separate native installation is not needed:

```powershell
docker compose --profile benchmarks run --rm k6
```

It defaults to a 30-second, 100-RPS check and requires the trained model. The report is saved in `reports/k6-scoring.json`. To use a native executable instead, follow the [official k6 installation instructions](https://grafana.com/docs/k6/latest/set-up/install-k6/). The workload uses [k6's gRPC client](https://grafana.com/docs/k6/latest/javascript-api/k6-net-grpc/client/).

First populate Redis using the generated input file and confirm scoring succeeds. Run the low-rate smoke load before attempting 5k RPS. Use the [benchmark guide](BENCHMARKS.md) for commands and reporting requirements.

## 4. Set up AWS only when ready to deploy

AWS is deferred for the current local setup. No AWS account or resource is needed to continue the local tests.

Required external choices and access:

- An AWS account with a budget alert and credentials configured locally through a role or AWS CLI profile. Do not put access keys in project files.
- Permission to create EC2 instances, security groups, IAM roles and instance profiles, and EBS storage.
- Terraform installed locally.
- A region, VPC ID, and private subnet ID. The subnet needs outbound access to package/image registries and connectivity to Systems Manager, through NAT or suitable endpoints and routing.
- A verified Ubuntu 24.04 amd64 AMI for that region with SSM Agent available. Verify publisher and architecture rather than copying an arbitrary AMI ID.
- An instance size and storage budget. The example uses `m6i.xlarge` and 80 GB encrypted gp3; neither its price nor its throughput is asserted here. Review current AWS prices before applying.
- AWS CLI and the Session Manager plugin for administration or local port forwarding. See [AWS Session Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html).

Copy `infra/terraform/terraform.tfvars.example` to `terraform.tfvars` in that directory and fill in your own values. Run `terraform init`, `terraform validate`, and `terraform plan` before `terraform apply`. Applying provisions billable resources. No AWS resources were created during implementation.

The template creates no inbound firewall rules and gives the instance a Systems Manager role. It installs Docker tooling; application source and model artifacts must then be transferred as described in the [runbook](RUNBOOK.md). The template does not create a VPC, NAT gateway, artifact bucket, or highly available Kafka cluster.

## 5. Evidence still to collect

The real-data baseline, Docker smoke test, and short k6/Redis checks have now run locally. See [VALIDATION.md](../reports/VALIDATION.md) for results and the separate crash-trial evidence. The next performance work is sustained throughput testing and improving the model and Redis latency; the current baseline misses both the AUC and Redis targets. AWS remains a later step. No additional native k6 or Grafana installation is required for the Compose workflow.
