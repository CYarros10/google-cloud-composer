resource "google_composer_environment" "composer1" {
  name   = "${var.composer_environment_name}"
  project = var.project_id
  region = var.region
  config {
    software_config {
      image_version = var.composer_image
    #  pypi_packages = var.composer_pypi_packages
    }
    node_count = var.worker_max_count
    node_config {
      network = var.composer_network
      service_account = var.composer_service_account
    }
  }
}
