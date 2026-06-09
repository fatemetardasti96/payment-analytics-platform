resource "aws_s3_bucket" "payments" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_policy" "payments" {
  bucket = aws_s3_bucket.payments.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowPaymentsRole"
      Effect = "Allow"
      Principal = {
        AWS = aws_iam_role.payments.arn
      }
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.payments.arn,
        "${aws_s3_bucket.payments.arn}/*"
      ]
    }]
  })
}
