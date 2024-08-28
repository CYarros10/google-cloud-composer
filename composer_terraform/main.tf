provider "google-beta" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "composer_api" {
  provider = google-beta
  project = var.project_id
  service = "composer.googleapis.com"
  disable_on_destroy = false
}

# module "composer-1" {
#   source                            = "./modules/composer1"
#   composer_environment_name         = "small-composer-unsupported"
#   composer_image                    = "composer-1.20.11-airflow-1.10.15"
#   project_id                        = var.project_id
#   region                            = var.region
#   composer_network                  = var.composer_network
#   composer_service_account          = var.composer_service_account
#   worker_max_count                  = var.worker_max_count
# }

# module "composer-alerting" {
#   source                            = "./modules/composer-alerting"  
#   project_id                        = var.project_id
#   notification_email                = var.notification_email
# }

module "composer-2" {
  source                            = "./modules/composer2"
  composer_environment_name         = "small-composer-latest"
  composer_image                    = var.composer_image
  project_id                        = var.project_id
  project_number                    = var.project_number
  region                            = var.region
  composer_network                  = var.composer_network
  composer_service_account          = var.composer_service_account
  composer_pypi_packages            = var.composer_pypi_packages
  environment_size                  = var.environment_size
  web_server_storage                = var.web_server_storage
  web_server_mem                    = var.web_server_mem
  web_server_cpu                    = var.web_server_cpu
  scheduler_storage                 = var.scheduler_storage
  scheduler_mem                     = var.scheduler_mem
  scheduler_cpu                     = var.scheduler_cpu
  scheduler_count                   = var.scheduler_count
  worker_max_count                  = var.worker_max_count
  worker_min_count                  = var.worker_min_count
  worker_storage                    = var.worker_storage
  worker_mem                        = var.worker_mem
  worker_cpu                        = var.worker_cpu
}

#module "composer-cicd" {
#  source                           = "./modules/composer-cicd"  
#  region                           = var.region
#  project_id                       = var.project_id
#  github_secret                    = var.github_secret
#}

#module "composer-roles" {
#  source                            = "./modules/composer-iam-roles"
#  project_id                        = var.project_id
#}