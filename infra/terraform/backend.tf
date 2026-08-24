terraform {
  # Supply bucket and prefix at init time. CI validation uses -backend=false.
  backend "gcs" {}
}
