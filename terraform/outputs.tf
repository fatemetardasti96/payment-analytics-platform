output "bucket_name" {
  value = aws_s3_bucket.payments.id
}

output "role_arn" {
  value = aws_iam_role.payments.arn
}
