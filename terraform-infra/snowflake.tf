locals {
  snowflake_tags = {
    Service-Name = "Snowflake"
  }
}

data "aws_iam_policy_document" "snowflake_s3_read" {
  statement {
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]

    resources = [
      aws_s3_bucket.datalake.arn,
      "${aws_s3_bucket.datalake.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "snowflake_s3_read" {
  name   = "snowflake-s3-read"
  policy = data.aws_iam_policy_document.snowflake_s3_read.json
}

#  IAM Role trust policy for Snowflake

data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [
        "arn:aws:iam::714551764970:user/96u71000-s"
      ]
    }

    actions = ["sts:AssumeRole"]

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = ["LD45029_SFCRole=4_Qxt7SGMLWhnkOlgndsXNRp1Ngko="]
    }
  }
}

resource "aws_iam_role" "snowflake_role" {
  name               = "SnowflakeReadS3Role"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role.json
  tags = merge(local.generic_tag, local.snowflake_tags)
}

#  Attach the S3 read policy to the role

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_read.arn
}
