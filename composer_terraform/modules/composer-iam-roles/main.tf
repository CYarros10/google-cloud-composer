resource "google_project_iam_custom_role" "composer-limited-user" {
  project     = var.project_id
  role_id     = "composerLimited"
  title       = "Composer Role Limited"
  description = "limited composer role with one iam permission"
  permissions = ["composer.environments.executeAirflowCommand"]
}