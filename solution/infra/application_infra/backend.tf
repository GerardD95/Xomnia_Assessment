terraform {
  backend "local" {
    path = "local_state/terraform.tfstate"
  }
}