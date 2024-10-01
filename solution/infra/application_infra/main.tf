terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.17.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Enable api's
resource "google_project_service" "cloud_sql" {
  service                    = "sqladmin.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "cloud_run" {
  service                    = "run.googleapis.com"
  disable_dependent_services = true
}

# Create cloud sql instance
resource "google_sql_database_instance" "main" {
  name             = var.cloud_sql_instance_name
  database_version = "POSTGRES_15"
  root_password    = var.db_pass
  depends_on       = [google_project_service.cloud_sql]

  settings {
    tier = "db-custom-2-3840"

    # Ip whitelisting
    ip_configuration {
      authorized_networks {
        name  = "home"
        value = var.home_ip
      }
    }
  }

  deletion_protection = false
}

# Create cloud run service
resource "google_cloud_run_service" "default" {
  name       = "my-service"
  depends_on = [google_sql_database_instance.main]
  location   = var.region

  template {
    spec {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/main-repo/shipping-api:latest"

        env {
          name  = "DB_URL"
          value = "postgresql://${var.db_user}:${var.db_pass}@/${var.db_name}?host=/cloudsql/${google_sql_database_instance.main.connection_name}"
        }
      }
    }

    # Add this block to connect to the Cloud SQL instance
    metadata {
      annotations = {
        "run.googleapis.com/cloudsql-instances"=google_sql_database_instance.main.connection_name
      }
    }
  }
}

# Allow public (unauthenticated) access to the Cloud Run service
resource "google_cloud_run_service_iam_member" "public" {
  service = google_cloud_run_service.default.name
  location = google_cloud_run_service.default.location
  role = "roles/run.invoker"
  member = "allUsers"
}

output "service_url" {
  description = "URL of the Cloud Run service"
  value       = google_cloud_run_service.default.status[0].url
}

output "cloud_sql_public_ip" {
  description = "Public IP of the Cloud SQL instance"
  value       = google_sql_database_instance.main.ip_address[0].ip_address
}