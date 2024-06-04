provider "google-beta" {
  project = var.project_id
  region  = var.region
}

resource "google_composer_environment" "composer2" {
  provider = google-beta
  name   = "${var.composer_environment_name}"
  project = var.project_id
  region = var.region
  config {
    software_config {
      image_version = var.composer_image
    #  pypi_packages = var.composer_pypi_packages
    }
    workloads_config {
      scheduler {
        cpu        = var.scheduler_cpu
        memory_gb  = var.scheduler_mem
        storage_gb = var.scheduler_storage
        count      = var.scheduler_count
      }
      web_server {
        cpu        = var.web_server_cpu
        memory_gb  = var.web_server_mem
        storage_gb = var.web_server_storage
      }
      worker {
        cpu = var.worker_cpu
        memory_gb  = var.worker_mem
        storage_gb = var.worker_storage
        min_count  = var.worker_min_count
        max_count  = var.worker_max_count
      }
    }
    node_config {
      network = var.composer_network
      service_account = var.composer_service_account
    }
    environment_size = var.environment_size
  }
}
