variable "project_id" {
  description = "The ID of the Google Cloud project"
  type        = string
}

variable "region" {
  description = "The region for the Google Cloud resources"
  type        = string
}

variable "zone" {
  description = "The region for the Google Cloud resources"
  type        = string
}

variable "cloud_sql_instance_name" {
  description = "The name of the Cloud SQL instance"
  type        = string
}

variable "db_name" {
  description = "The name of the database"
  type        = string
}

variable "db_user" {
  description = "The username for the database"
  type        = string
}

variable "db_pass" {
  description = "The password for the database"
  type        = string
  sensitive   = true
}

variable "home_ip" {
  description = "The IP address to whitelist for the database"
  type        = string
}

variable "image_tag" {
  description = "The tag of the Docker image to deploy"
  type        = string
}