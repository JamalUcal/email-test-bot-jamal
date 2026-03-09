
# Upload core config
gsutil cp ./config/core_config.json gs://$BUCKET_NAME/config/core_config.json

# Upload supplier config (adapted from original)
gsutil cp ./config/supplier_config.json gs://$BUCKET_NAME/config/supplier_config.json

# Upload brand config (adapted from original)
gsutil cp ./config/brand_config.json gs://$BUCKET_NAME/config/brand_config.json