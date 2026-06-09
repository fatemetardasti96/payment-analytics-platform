data "aws_iam_user" "payment" {
  user_name = "payment-user"
}

resource "aws_iam_user_policy" "assume_payments_role" {
  name = "assume-payments-s3-role"
  user = data.aws_iam_user.payment.user_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "sts:AssumeRole"
      Resource = aws_iam_role.payments.arn
    }]
  })
}

resource "aws_iam_role" "payments" {
  name = "payments-s3-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "payments_s3" {
  name = "payments-s3-access"
  role = aws_iam_role.payments.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.payments.arn, "${aws_s3_bucket.payments.arn}/*"]
    }]
  })
}
