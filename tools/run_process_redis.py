import struct
from functools import partial
import asyncio
import redis
import requests
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from pathlib import Path
import yaml

with open('process_redis.yaml') as f:
    config = yaml.safe_load(f)
# 初始化 Redis 连接
redis_client = redis.Redis(
    host=config['redis']['host'],
    port=config['redis']['port'],
    db=config['redis']['db'],
    password=config['redis']['password']
)

# 初始化 OSS 客户端

bucket = config['cos']['bucket']
cosConfig = CosConfig(
    Region=config['cos']['region'],
    SecretId=config['cos']['secret_id'],
    SecretKey=config['cos']['secret_key']
)
client = CosS3Client(cosConfig)
redis_queue = 'voiceTTSTaskQueue'


async def download_file(url, local_path):
    """
    异步下载文件
    """
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)
    return local_path


async def upload_to_cos(local_path, oss_key):
    """
    异步上传文件到 cos
    """
    response = client.upload_file(
        Bucket=bucket,
        Key=oss_key,
        LocalFilePath=local_path,
        EnableMD5=False,
        progress_callback=None
    )


def process_redis_queue():
    """
    处理 redis 队列
    """
    print('开始处理redis队列')
    while True:
        # 从redis队列中获取任务
        task = redis_client.blpop('video_TTS_task_queue', timeout=0)
        if task:
            print(task)
            task_data = eval(task[1].decode('utf-8'))
            audio_file_url = task_data.get('audioFileUrl')
            audio_text = task_data.get('audioText')
            person_id = task_data.get('personId')
            content = task_data.get('content')

            ref_folder = Path("references") / str(person_id)
            ref_folder.mkdir(parents=True, exist_ok=True)

            local_file_path = ref_folder / Path(audio_file_url).name
            if not local_file_path.exists() or local_file_path.stat().st_size == 0:
                # 下载文件
                response = requests.get(audio_file_url)
                with open(local_file_path, 'wb') as f:
                    f.write(response.content)
            print('文件下载完成')
            # 进行语音推理
            # new_audio_path = speech_inference(str(local_file_path), audio_text, person_id, content)

            # 推理完成后，更新redis状态
            # task_status_key = f"task_status:{person_id}"
            # redis_client.set(task_status_key, "completed")
            # 将生成文件地址塞入上传文件的队列
            # redis_client.rpush('upload_queue', new_audio_path)


# 功能2：通过上传文件的队列，获取文件地址并上传
def process_upload_queue():
    r = redis.Redis(host='localhost', port=6379, db=0)
    while True:
        # 从上传队列中获取文件地址
        file_path = r.blpop('upload_queue', timeout=0)
        if file_path:
            file_path = file_path[1].decode('utf-8')
            # 进行COS文件上传
            uploaded_url = upload_to_cos(file_path)

            # 上传完成后，改变redisKey中的状态值
            person_id = Path(file_path).stem.split('_')[-1]
            task_status_key = f"task_status:{person_id}"
            r.set(task_status_key, "uploaded")


if __name__ == "__main__":
    import threading

    # 启动两个线程分别处理两个功能
    t1 = threading.Thread(target=process_redis_queue)
    # t2 = threading.Thread(target=process_upload_queue)

    t1.start()
    # t2.start()

    t1.join()
    # t2.join()