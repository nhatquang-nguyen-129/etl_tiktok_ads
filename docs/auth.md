# Authentication for TikTok Ads

## Purpose

- Authenticate **Google Cloud Platform** services used in this pipeline

- Authenticate **TikTok Ads Business API V1.3** with access token

- Use manual login with **Application Default Credentials** for local environment

- Use **Service Account** authentication to manage permissions in cloud environments

- Use centralized Google Cloud Project with required APIs enabled for cloud deployment

---

## Local setup

### Local setup for Windows

- Download and install Google Cloud SDK from official source
```bash
https://cloud.google.com/sdk
```

- Verify installed Google Cloud SDK version
```bash
gcloud --version
```

- Login to Google Cloud on your Windows local environment
```bash
gcloud auth login
```

- Login and create **Application Default Credentials** (ADC) used by Google BigQueryAirflow/dbt/Terraform or any other Google Cloud client libraries
```bash
gcloud auth application-default login
```

- Verify authenticated Google accounts
```bash
gcloud auth list
```

- Check all accessible Google Cloud projects attached to the current ADC
```bash
gcloud projects list
```

- Set default Google Cloud project for Google BigQuery and quota billing
```bash
gcloud auth application-default set-quota-project YOUR_GOOGLE_CLOUD_PROJECT_ID
```

- Check Google Cloud quota project attached to ADC
```bash
gcloud config get-value project
```

- Verify ADC is working
```bash
gcloud auth application-default print-access-token
```

---

### Local setup for MacOS

- Install **Homebrew** from official source
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

- Add **Homebrew** to your system path once the installation finishes if you're using an **Apple Silicon Mac with M chip**
```bash
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

- Verify Homebrew version
```bash
brew --version
```

- Download and install Google Cloud SDK from official source
```bash
brew install --cask google-cloud-sdk
```

- Verify installed Google Cloud SDK version
```bash
gcloud --version
```

- Login and create **Application Default Credentials** (ADC) used by Google BigQueryAirflow/dbt/Terraform or any other Google Cloud client libraries
```bash
gcloud auth application-default login
```

- Verify authenticated Google accounts
```bash
gcloud auth list
```

- Check all accessible Google Cloud projects attached to the current ADC
```bash
gcloud projects list
```

- Set default Google Cloud project for Google BigQuery and quota billing
```bash
gcloud auth application-default set-quota-project YOUR_GOOGLE_CLOUD_PROJECT_ID
```

- Check Google Cloud quota project attached to ADC
```bash
gcloud config get-value project
```

- Verify ADC is working
```bash
gcloud auth application-default print-access-token
```

## Cloud Run setup

### Enable minimum required APIs and services

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Cloud Run API** for container execution in the target Google Cloud project

- Enable **Google BigQuery API** for data warehouse access in the target Google Cloud project

---

### Enable Service Account

- Create a dedicated Google Cloud Platform's **Service Account** for pipeline_recon_ads

- Grant **Cloud Run Admin permissions** for required IAM Roles

- Grant **BigQuery Data Editor** and **BigQuery Job User** for required IAM Roles