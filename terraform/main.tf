terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 5.0"
          }
                     }


# The 'b%sf2' is the bucket name 
# is just a random five-char
# substring:
  backend "s3" {
    bucket = "totesys-state-bucket-b%sf2"
    key = "state.tf"
    region = "eu-west-2"
              }
         }





provider "aws" {
  region = "eu-west-2"
               }




# Provision the buckets
# (the last five chars in the
# names of the buckets are 
# random):


module "processed_bucket" {
  source      = "./buckets-module/main.tf"
  bucket_name = "totesys-processed-bucket-H{%,O"
                          }

module "code_bucket" {
  source      = "./buckets-module/main.tf"
  bucket_name = "totesys-code-bucket-4j£f~"
                     }


module "ingestion_bucket" {
  source      = "./buckets-module/main.tf"
  bucket_name = "totesys-ingestion-bucket-[7^d6"
                          }













# data "aws_caller_identity" "current" {}

# data "aws_region" "current" {}