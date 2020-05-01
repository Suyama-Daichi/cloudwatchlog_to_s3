import boto3
import datetime

# ロググループを検索する際のクエリ―
PREFIX = '/aws/lambda/hmkvDementiaFunction'
# ログを保管するs3バケット
S3_BUCKET = 'hmkv-test-log-store'
# ログを保管するs3のディレクトリ
S3_DIR = 'logs'


def lambda_handler(event=None, context=None):

    client = boto3.client('logs')
    # ロググループ取得
    log_group = client.describe_log_groups(
        logGroupNamePrefix=PREFIX)['logGroups'][0]

    # 実行日時の0:00:00~23:59:59を取得
    now = datetime.datetime.now()
    start_day = datetime.datetime(now.year, now.month, now.day, 0, 0, 0)
    end_day = datetime.datetime(now.year, now.month, now.day, 23, 59, 59)

    # エクスポートタスクを作成
    response = client.create_export_task(taskName='test_task', logGroupName=log_group['logGroupName'], fromTime=int(
        start_day.timestamp() * 1000), to=int(end_day.timestamp() * 1000), destination=S3_BUCKET, destinationPrefix=S3_DIR)

    return response


lambda_handler()
