terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.44.1"
    }
  }
}

provider "google" {
  # Configuration options
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

resource "random_id" "name_suffix" {
  byte_length = 4
}
resource "google_storage_bucket" "sample" {
    force_destroy               = true
    location                    = var.region
    name                        = "airflow_pipeline_uploads-${random_id.name_suffix.hex}"
    project                     = var.project_id
}

resource "google_sql_database_instance" "main" {
    database_version               = var.database_version
    deletion_protection            = true
    name                           = "airflow-db-instance"
    project = var.project_id
    region  = var.region
    root_password = var.root_password

    settings {
        activation_policy     = "ALWAYS"
        availability_type     = "ZONAL"
        disk_size             = 10
        disk_type             = "PD_HDD"
        tier                  = "db-f1-micro"

        ip_configuration {
            ipv4_enabled = true
            require_ssl  = false
        }

        location_preference {
            zone = var.zone
        }
    }
}
resource "google_sql_database" "database" {
  name     = "airflow_db"
  instance = google_sql_database_instance.main.name
}
resource "google_service_account" "my_sa" {
    account_id   = "twitter"
    description  = "For extracting, analyzing, and displaying twitter data"
}



