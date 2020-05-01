import boto3
import datetime

# ロググループを検索する際のクエリ―
PREFIX = '/aws/lambda/hmkvDementiaFunction'
# ログを保管するs3バケット
S3_BUCKET = 'hmkv-test-log-store'
# ログを保管するs3のディレクトリ
S3_DIR = 'logs'
# エクスポートタスク名
EXPORT_TASK_NAME = "test_task"


def lambda_handler(event=None, context=None):

    client = boto3.client('logs')
    # ロググループ取得
    log_group = client.describe_log_groups(
        logGroupNamePrefix=PREFIX)['logGroups'][0]

    # ログストリーム取得
    log_stoream_response = client.describe_log_streams(
        logGroupName=log_group['logGroupName'],
        orderBy='LastEventTime',
        descending=False,
        limit=50
    )
    log_storeams = log_stoream_response['logStreams']
    # ロググループ内の全ログストリームを取得する
    while "nextToken" in log_stoream_response:
        log_stoream_response = client.describe_log_streams(
            logGroupName=log_group['logGroupName'],
            orderBy='LastEventTime',
            descending=False,
            limit=50,
            nextToken=log_stoream_response['nextToken']
        )
        log_storeams.extend(log_stoream_response['logStreams'])

    # 実行日時の0:00:00~23:59:59を取得
    now = datetime.datetime.now()
    start_day = datetime.datetime(now.year, 3, now.day, 0, 0, 0)
    end_day = datetime.datetime(now.year, now.month, now.day, 23, 59, 59)

    # エクスポートタスクを作成
    response = client.create_export_task(
        taskName=EXPORT_TASK_NAME,
        logGroupName=log_group['logGroupName'],
        fromTime=int(start_day.timestamp() * 1000),
        to=int(end_day.timestamp() * 1000),
        destination=S3_BUCKET, destinationPrefix=S3_DIR
    )

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        # 転送したログストリームを削除する
        for log_stoream in log_storeams:
            client.delete_log_stream(
                logGroupName=log_group['logGroupName'],
                logStreamName=log_stoream['logStreamName']
            )

    return response


lambda_handler()
