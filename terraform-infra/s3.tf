locals {
  service = {
    Service-Name = "Airflow"
  }
}


resource "aws_s3_bucket" "spectrum_bucket" {
  bucket = "coretelecoms-customers"

  tags = merge(
    local.service,
    local.generic_tag
  )
}


resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.spectrum_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

#  create folders in S3
resource "aws_s3_object" "raw_folder" {
  bucket = aws_s3_bucket.spectrum_bucket.bucket
  key    = "raw/"
  
}