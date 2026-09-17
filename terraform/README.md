# Terraform Infrastructure

This directory contains Terraform configuration for the
Secure Employee Data Platform.

## Managed Infrastructure

Terraform is configured to manage the Google Cloud APIs required
by the project:

- BigQuery
- Dataflow
- Dataform
- Cloud Composer
- Secret Manager
- Cloud Logging
- Cloud Monitoring

## Files

- `provider.tf` — Terraform and Google Cloud provider configuration
- `variables.tf` — configurable project variables
- `main.tf` — required Google Cloud APIs
- `outputs.tf` — Terraform outputs

## Usage

Initialize Terraform:

```bash
terraform init