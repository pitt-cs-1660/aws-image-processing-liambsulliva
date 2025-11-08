import json
from PIL import Image
import io
import boto3
from pathlib import Path

def download_from_s3(bucket, key):
    s3 = boto3.client('s3')
    buffer = io.BytesIO()
    s3.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return Image.open(buffer)

def upload_to_s3(bucket, key, data, content_type='image/jpeg'):
    s3 = boto3.client('s3')
    if isinstance(data, Image.Image):
        buffer = io.BytesIO()
        data.save(buffer, format='JPEG')
        buffer.seek(0)
        s3.upload_fileobj(buffer, bucket, key)
    else:
        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

def exif_handler(event, context):
    """
    EXIF Lambda - Process all images in the event
    """
    print("EXIF Lambda triggered")
    print(f"Event received with {len(event.get('Records', []))} SNS records")

    processed_count = 0
    failed_count = 0

    for sns_record in event.get('Records', []):
        try:
            sns_message = json.loads(sns_record['Sns']['Message'])

            for s3_event in sns_message.get('Records', []):
                try:
                    s3_record = s3_event['s3']
                    bucket_name = s3_record['bucket']['name']
                    object_key = s3_record['object']['key']

                    print(f"Processing: s3://{bucket_name}/{object_key}")

                    image = download_from_s3(bucket_name, object_key)

                    exif_data = image._getexif()
                    exif_dict = {}

                    if exif_data is not None:
                        from PIL.ExifTags import TAGS
                        for tag_id, value in exif_data.items():
                            tag = TAGS.get(tag_id, tag_id)
                            if isinstance(value, bytes):
                                try:
                                    value = value.decode(errors="replace")
                                except Exception:
                                    value = str(value)
                            exif_dict[tag] = value
                    else:
                        print(f"No EXIF data found for {object_key}")

                    key_parts = Path(object_key)
                    parts = key_parts.parts
                    if len(parts) > 1:
                        new_key = str(Path('exif', *parts[1:])).rsplit('.', 1)[0] + '.json'
                    else:
                        new_key = str(Path('exif', key_parts.name)).rsplit('.', 1)[0] + '.json'

                    print(f"Saving EXIF data to s3://{bucket_name}/{new_key}")

                    upload_to_s3(bucket_name, new_key, json.dumps(exif_dict, indent=2), content_type='application/json')

                    processed_count += 1

                except Exception as e:
                    failed_count += 1
                    error_msg = f"Failed to process {object_key}: {str(e)}"
                    print(error_msg)

        except Exception as e:
            print(f"Failed to process SNS record: {str(e)}")
            failed_count += 1

    summary = {
        'statusCode': 200 if failed_count == 0 else 207,
        'processed': processed_count,
        'failed': failed_count,
    }

    print(f"Processing complete: {processed_count} succeeded, {failed_count} failed")
    return summary
