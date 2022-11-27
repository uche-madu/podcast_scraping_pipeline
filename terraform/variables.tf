variable "project_id" {
  description = "GCP project id"
  default = "twitter-pipeline-366415"
}
variable "region" {
  description = "GCP region"
  default = "europe-west4"
}
variable "zone" {
  description = "GCP zone"
  default = "europe-west4-a"
}
variable "database_version" {
  description = "POSTGRESQL version"
  default = "POSTGRES_14"
}
variable "root_password" {
  description = "Password for postgres root user"
  default = "abcABC123!"
}