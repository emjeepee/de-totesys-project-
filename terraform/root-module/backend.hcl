# text will appear white if 
# VS Code doesn’t recognize HCL,
# which it won't if you haven't 
# installed VS Code extension 
# HashiCorp Terraform.

bucket         = env("AWS_STATE_BUCKET")
key            = "terraform/state.tfstate"
region         = "eu-west-2"
