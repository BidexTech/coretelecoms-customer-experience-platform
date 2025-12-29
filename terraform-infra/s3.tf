locals {
  service = {
    Service-Name = "Airflow"
  }
}


resource "aws_s3_bucket" "datalake" {
  # provider = aws.personal_acoount
  bucket = "coretelecoms-datalake-raw"

  tags = merge(
    local.service,
    local.generic_tag
  )
}


resource "aws_s3_bucket_versioning" "versioning_example" {
  # provider = aws.personal_acoount
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

#  create folders in S3
resource "aws_s3_object" "raw_folder" {
  # provider = aws.personal_acoount
  bucket = aws_s3_bucket.datalake.bucket
  key    = "raw/"

}