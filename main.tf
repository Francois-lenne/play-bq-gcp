provider "google" {
  project = "votre-projet-id"
  region  = "votre-region"
}

resource "google_cloudfunctions_function" "default" {
  name        = "ma-fonction"
  description = "Ma fonction Cloud"
  runtime     = "python310"

  available_memory_mb   = 256
  source_repository_url = "https://source.developers.google.com/projects/votre-projet-id/repos/votre-repo/moveable-aliases/master/paths/"

  trigger_http = true

  entry_point = "votre_point_d_entree"
}

resource "google_cloud_scheduler_job" "default" {
  name     = "ma-tache-planifiee"
  schedule = "0 8 * * *"

  http_target {
    http_method = "GET"
    uri         = google_cloudfunctions_function.default.https_trigger_url
  }
}