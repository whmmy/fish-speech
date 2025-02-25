import datetime
import io
import urllib.request
from pathlib import Path

import redis
import requests
import soundfile as sf
import yaml
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

from fish_speech.utils.schema import ServeTTSRequest, ServeReferenceAudio
from tools.redisprocess.ret import *
from tools.server.model_manager import ModelManager

with open('./process_redis.yaml') as f:
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
cosClient = CosS3Client(cosConfig)
redis_queue = 'TTS:TaskQueue'


def createEngine() -> ModelManager:
    return ModelManager(
        mode="tts",
        device='cuda',
        half=True,
        compile=True,
        asr_enabled=False,
        llama_checkpoint_path="checkpoints/fish-speech-1.5",
        decoder_checkpoint_path="checkpoints/fish-speech-1.5/firefly-gan-vq-fsq-8x1024-21hz-generator.pth",
        decoder_config_name="firefly_gan_vq",
    )


async def download_file(url, local_path):
    """
    异步下载文件
    """
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)
    return local_path


def download_file_bytes(url):
    try:
        # 打开 URL 并获取响应
        with urllib.request.urlopen(url) as response:
            # 读取响应内容，返回的是 bytes 类型
            return response.read()
    except Exception as e:
        print(f"获取 URL 内容时出现错误: {e}")
        return None


async def upload_to_cos(local_path, oss_key):
    """
    异步上传文件到 cos
    """
    response = cosClient.upload_file(
        Bucket=bucket,
        Key=oss_key,
        LocalFilePath=local_path,
        EnableMD5=False,
        progress_callback=None
    )


def upload_bytes_to_cos(audioBytes, oss_key):
    """
    异步上传文件到 cos
    """
    print(f'开始上传文件到 cos, oss_key:{oss_key}')
    audioBytes.seek(0)
    response = cosClient.put_object(
        Bucket=bucket,
        Key=oss_key,
        Body=audioBytes,
        EnableMD5=False,
    )


def process_redis_queue():
    modelManager = createEngine()
    """
    处理 redis 队列
    """
    print('开始处理redis队列')
    while True:
        # 从redis队列中获取任务
        task = redis_client.blpop([redis_queue], timeout=0)
        if task:
            ret = JsonRet()
            print(task)
            # 判断是否是json且合法格式
            
            task_data_str = task[1].decode('utf-8')
            task_dict = json.loads(task_data_str)
            audio_file_url = task_dict.get('audioFileUrl')
            audio_text = task_dict.get('audioText')
            person_id = task_dict.get('personId')
            taskId = task_dict.get('taskId')
            content = task_dict.get('content')

            task_result_key = f'TTS:task_result:{taskId}'
            audioBytes = download_file_bytes(audio_file_url)
            if audioBytes is None:
                ret.set_code(ret.RET_FAIL)
                ret.set_msg(f'文件不存在，或者下载错误,url:{audio_file_url}')
                putTaskStatus(task_result_key, ret)
                continue
            print(f'文件获取完成，{len(audioBytes)} 字节的内容')
            ref = ServeReferenceAudio(audio=audioBytes, text=audio_text)
            # 进行语音推理
            engine = modelManager.tts_inference_engine
            req = ServeTTSRequest(
                text=content,
                references=[ref],
                max_new_tokens=1024,
                chunk_length=200,
                top_p=0.7,
                repetition_penalty=1.2,
                temperature=0.7,
                seed=None,
                use_memory_cache="off",
            )
            audio, errMsg = inference_wrapper(engine, req)
            print(f'推理完成，{len(audio)} 字节的内容,{errMsg}')
            # 推理完成后，更新redis状态
            if audio is None:
                ret.set_code(ret.RET_FAIL)
                ret.set_msg(errMsg)
                putTaskStatus(task_result_key, ret)
            else:
                sample_rate, audio_data = audio
                buffer = io.BytesIO()
                sf.write(
                    buffer,
                    audio_data,
                    sample_rate,
                    format=req.format,
                )
                fakeAudioFileName = generate_filename(person_id, taskId, req.format)
                upload_bytes_to_cos(buffer, fakeAudioFileName)
                ret.set_code(ret.RET_OK)
                ret.set_data({
                    "fileKey": cosClient.get_object_url(bucket, fakeAudioFileName)
                })
                putTaskStatus(task_result_key, ret)
        # 将生成文件地址塞入上传文件的队列


def putTaskStatus(key, ret: JsonRet):
    print(f'更新redis状态,key:{key},ret:{ret()}')
    redis_client.set(key, ret())


def inference_wrapper(engine, request):
    for result in engine.inference(request):
        print(f'推理结果:{result}')
        match result.code:
            case "final":
                return result.audio, None
            case "error":
                return None, result.error
            case _:
                pass


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


def generate_filename(person_id, taskId, file_format):
    """
   生成包含时间、person_id、taskId和文件格式的随机文件名
   :param person_id: 人员ID
   :param taskId: 任务ID
   :param file_format: 文件格式，如 'jpg', 'png', 'pdf' 等
   :return: 生成的随机文件名
   """
    # 获取当前时间并格式化为字符串，只保留到分钟
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%M")
    # 组合时间、person_id、taskId和文件格式生成文件名
    filename = f"/TTS/TASK_RESULT/{current_time}_{person_id}_{taskId}.{file_format}"
    return filename


if __name__ == "__main__":
    import threading

    # 启动两个线程分别处理两个功能
    t1 = threading.Thread(target=process_redis_queue)
    # t2 = threading.Thread(target=process_upload_queue)

    t1.start()
    # t2.start()

    t1.join()
    # t2.join()
