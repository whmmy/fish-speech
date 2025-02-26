import datetime
import io
import urllib.request
from pathlib import Path
import logging

import redis
import soundfile as sf
import yaml
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client

from fish_speech.utils.schema import ServeTTSRequest, ServeReferenceAudio
from tools.redisprocess.ret import *
from tools.server.model_manager import ModelManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config(file_path: str):
    """加载配置文件"""
    try:
        with open(file_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"配置文件 {file_path} 未找到")
        raise
    except yaml.YAMLError as e:
        logging.error(f"解析配置文件 {file_path} 时出错: {e}")
        raise


def create_directory(path: str):
    """创建目录"""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def init_redis_client(config: dict):
    """初始化 Redis 连接"""
    try:
        return redis.Redis(
            host=config['redis']['host'],
            port=config['redis']['port'],
            db=config['redis']['db'],
            password=config['redis']['password']
        )
    except redis.RedisError as e:
        logging.error(f"初始化 Redis 连接时出错: {e}")
        raise


def init_cos_client(config: dict):
    """初始化 COS 客户端"""
    try:
        bucket = config['cos']['bucket']
        cosConfig = CosConfig(
            Region=config['cos']['region'],
            SecretId=config['cos']['secret_id'],
            SecretKey=config['cos']['secret_key'],
        )
        return CosS3Client(cosConfig), bucket
    except Exception as e:
        logging.error(f"初始化 COS 客户端时出错: {e}")
        raise


# 加载配置
processConfig = load_config('./process_redis.yaml')
# 初始化 Redis 连接
redis_client = init_redis_client(processConfig)
# 初始化 OSS 客户端
cosClient, bucket = init_cos_client(processConfig)
redis_queue = 'VVS:TTS:TaskQueue'


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


def download_file_bytes(url: str):
    """
    下载文件并返回字节内容
    :param url: 文件的URL
    :return: 文件的字节内容，如果出错则返回 None
    """
    try:
        # 打开 URL 并获取响应
        with urllib.request.urlopen(url) as response:
            # 读取响应内容，返回的是 bytes 类型
            return response.read()
    except Exception as e:
        logging.error(f"获取 URL 内容时出现错误: {e}")
        return None


def upload_bytes_to_cos(audioBytes: io.BytesIO, oss_key: str) -> bool:
    """
    异步上传文件到 cos
    :param audioBytes: 音频文件的字节流
    :param oss_key: 上传到 COS 的对象键
    :return: 上传是否成功
    """
    try:
        logging.info(f'开始上传文件到 cos, oss_key:{oss_key}')
        audioBytes.seek(0)
        response = cosClient.put_object(
            Bucket=bucket,
            Key=oss_key,
            Body=audioBytes,
            EnableMD5=False,
        )
        return True
    except Exception as e:
        logging.error(f"上传文件 {oss_key} 到 COS 失败: {e}")
        return False


def process_redis_queue():
    modelManager = createEngine()
    logging.info('开始处理redis队列')
    while True:
        # 从redis队列中获取任务
        task = redis_client.blpop([redis_queue], timeout=0)
        if task:
            ret = JsonRet()
            task_data_str = task[1].decode('utf-8')
            try:
                task_dict = json.loads(task_data_str)
            except json.JSONDecodeError:
                logging.error(f"解析任务数据时出错: {task_data_str}")
                continue
            audio_file_url = task_dict.get('audioFileUrl')
            audio_text = task_dict.get('audioText')
            person_id = task_dict.get('personId')
            taskId = task_dict.get('taskId')
            content = task_dict.get('content')
            try:

                task_result_key = f'TTS:task_result:{taskId}'
                audioBytes = download_file_bytes(audio_file_url)
                if audioBytes is None:
                    ret.set_code(ret.RET_FAIL)
                    ret.set_msg(f'文件不存在，或者下载错误,url:{audio_file_url}')
                    putTaskStatus(task_result_key, ret)
                    putTaskResult(1, f'文件不存在，或者下载错误,url:{audio_file_url}', taskId)
                    continue
                logging.info(f'文件获取完成，{len(audioBytes)} 字节的内容')
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
                logging.info(f'推理完成，{len(audio)} 字节的内容,{errMsg}')
                # 推理完成后，更新redis状态
                if audio is None:
                    ret.set_code(ret.RET_FAIL)
                    ret.set_msg(errMsg)
                    putTaskStatus(task_result_key, ret)
                    putTaskResult(1, errMsg, taskId)
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
                    if upload_bytes_to_cos(buffer, fakeAudioFileName):
                        ret.set_code(ret.RET_OK)
                        ret.set_data({
                            "fileKey": cosClient.get_object_url(bucket, fakeAudioFileName)
                        })
                        putTaskResult(0, 'success', taskId, cosClient.get_object_url(bucket, fakeAudioFileName))
                    else:
                        ret.set_code(ret.RET_FAIL)
                        ret.set_msg(f"上传文件 {fakeAudioFileName} 到 COS 失败")
                        putTaskResult(1, f"上传文件 {fakeAudioFileName} 到 COS 失败", taskId)
                    putTaskStatus(task_result_key, ret)
            except Exception as e:
                logging.error(f"处理 Redis 队列任务时出错: {e}")
                putTaskResult(1, f"处理 Redis 队列任务时出错: {e}", taskId)


def putTaskStatus(key: str, ret: JsonRet):
    logging.info(f'更新redis状态,key:{key},ret:{ret()}')
    redis_client.set(key, ret())


def putTaskResult(code, msg, taskId, fileUrl=None):
    data = {
        'code': code,
        'msg': msg,
        'taskId': taskId,
        'fileUrl': fileUrl
    }
    redis_client.rpush('VVS:GPU_TASK_RESULT_QUEUE', json.dumps(data))


def inference_wrapper(engine, request):
    for result in engine.inference(request):
        logging.info(f'推理结果:{result}')
        match result.code:
            case "final":
                return result.audio, None
            case "error":
                return None, result.error
            case _:
                pass


def generate_filename(person_id: str, taskId: str, file_format: str):
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

    t1 = threading.Thread(target=process_redis_queue)

    t1.start()

    t1.join()
