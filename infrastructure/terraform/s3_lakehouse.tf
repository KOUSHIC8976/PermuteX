
resource "aws_s3_bucket" "permutex_lakehouse" {
  bucket = "permutex-lakehouse-${var.environment}-98765" # Must be globally unique
  

  lifecycle {
    prevent_destroy = true 
  }
}


resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse_crypto" {
  bucket = aws_s3_bucket.permutex_lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}


resource "aws_s3_bucket_public_access_block" "lakehouse_privacy" {
  bucket = aws_s3_bucket.permutex_lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


resource "aws_s3_object" "bronze_layer" {
  bucket       = aws_s3_bucket.permutex_lakehouse.id
  key          = "bronze/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "silver_layer" {
  bucket       = aws_s3_bucket.permutex_lakehouse.id
  key          = "silver/"
  content_type = "application/x-directory"
}

resource "aws_s3_object" "gold_layer" {
  bucket       = aws_s3_bucket.permutex_lakehouse.id
  key          = "gold/"
  content_type = "application/x-directory"
}