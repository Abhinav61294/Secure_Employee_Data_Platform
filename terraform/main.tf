resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "dataflow.googleapis.com",
    "dataform.googleapis.com",
    "composer.googleapis.com",
    "secretmanager.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com"
  ])

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}