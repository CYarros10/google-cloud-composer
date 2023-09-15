// Create a secret containing the personal access token and grant permissions to the Service Agent
resource "google_secret_manager_secret" "github_token_secret" {
    project =  var.project_id
    secret_id = "composer-cicd-test-github-pat"

    replication {
        automatic = true
    }
}

resource "google_secret_manager_secret_version" "github_token_secret_version" {
    secret = google_secret_manager_secret.github_token_secret.id
    secret_data = var.github_secret
}

#data "google_iam_policy" "serviceagent_secretAccessor" {
#    binding {
#        role = "roles/secretmanager.secretAccessor"
#        members = ["serviceAccount:service-PROJECT_NUMBER@gcp-sa-cloudbuild.iam.gserviceaccount.com"]
#    }
#}

#resource "google_secret_manager_secret_iam_policy" "policy" {
#  project = google_secret_manager_secret.github_token_secret.project
#  secret_id = google_secret_manager_secret.github_token_secret.secret_id
#  policy_data = data.google_iam_policy.serviceagent_secretAccessor.policy_data
#}

// Create the GitHub connection
resource "google_cloudbuildv2_connection" "my_connection" {
    project = var.project_id
    location = var.region
    name = "composer-cicd-repo-connection"

    github_config {
        app_installation_id = "30318827"
        authorizer_credential {
            oauth_token_secret_version = google_secret_manager_secret_version.github_token_secret_version.id
        }
    }
    #depends_on = [google_secret_manager_secret_iam_policy.policy]
}

resource "google_cloudbuildv2_repository" "my-repository" {
  name = "composer-cicd-repo"
  project = var.project_id
  location = var.region
  parent_connection = google_cloudbuildv2_connection.my_connection.name
  remote_uri = "https://github.com/CYarros10/composer-cicd.git"
}

resource "google_cloudbuild_trigger" "repo-trigger" {
  name = "composer-cicd-trigger"
  project = var.project_id
  location = var.region
  
  repository_event_config {
    repository = google_cloudbuildv2_repository.my-repository.id
    push {
      branch = "main"
    }
  }

  substitutions = {
    _DAGS_DIRECTORY = "dags/"
    _DAGS_BUCKET = "us-central1-composer-2-smal-a2257660-bucket"
  }

  filename = "cloudbuild.yaml"
}