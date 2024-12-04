import re, os
import json
import requests
import time, glob
import csv
import logging
from colorlog import ColoredFormatter
import cv2  # 使用 OpenCV 进行图像拼接
from datetime import datetime
from ratelimit import limits, sleep_and_retry

RETRY_COUNT = 3  # 最大重试次数
RETRY_DELAY = 5  # 每次重试之间的间隔时间，单位为秒

# region 配置日志
log_format = "%(asctime)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(log_format)

# 获取当前运行时间并格式化为文件名
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_dir = "log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 日志文件路径
log_file = os.path.join(log_dir, f"run_log_{current_time}.log")

# 创建文件处理器用于将日志写入文件
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)

# 创建控制台处理器用于控制台输出带颜色的日志
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter("%(log_color)s" + log_format))

# 获取日志记录器并设置级别
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
# endregion

# write csv
def write_csv(filepath, data, head=None, mode='w'):
    if head and mode == 'w':
        data = [head] + data
    with open(filepath, mode=mode, encoding='UTF-8-sig', newline='') as f:
        writer = csv.writer(f)
        if head and mode == 'w':
            writer.writerow(head)
        for i in data:
            writer.writerow(i)

# read csv
def read_csv(filepath):
    data = []
    if os.path.exists(filepath):
        with open(filepath, mode='r', encoding='utf-8') as f:
            lines = csv.reader(f)  # 此处读取到的数据是将每行数据当做列表返回的
            for line in lines:
                data.append(line)
        return data
    else:
        logger.error('filepath is wrong: {}'.format(filepath))
        return []

# 下载图片并带有重试机制
def grab_img_baidu_with_retry(_url, _headers=None):
    if _headers is None:
        # 设置请求头 request header
        headers = {
            "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
            "Referer": "https://map.baidu.com/",
            "sec-ch-ua-mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
    else:
        headers = _headers

    for attempt in range(RETRY_COUNT):
        try:
            response = requests.get(_url, headers=headers)
            if response.status_code == 200 and response.headers.get('Content-Type') == 'image/jpeg':
                return response.content
            else:
                logger.warning(
                    f"Download failed for {_url}, status code: {response.status_code}, attempt {attempt + 1}/{RETRY_COUNT}")
        except Exception as e:
            logger.error(f"Error during downloading image for {_url}, attempt {attempt + 1}/{RETRY_COUNT}: {str(e)}")

        if attempt < RETRY_COUNT - 1:
            logger.info(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

    logger.error(f"Failed to download image after {RETRY_COUNT} attempts for {_url}")
    return None

def openUrl(_url):
    # 设置请求头 request header
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
    }
    try:
        response = requests.get(_url, headers=headers)
        if response.status_code == 200:
            return response.content
        else:
            logger.error(f"Request to {_url} returned status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error requesting {_url}: {str(e)}")
        return None

def getPanoId_with_retry(_lng, _lat):
    # 获取百度街景中的svid, 带重试机制
    url_template = "https://mapsv0.bdimg.com/?&qt=qsdata&x={}&y={}&l=17.031000000000002&action=0&mode=day&t=1530956939770"
    url = url_template.format(str(_lng), str(_lat))

    for attempt in range(RETRY_COUNT):
        try:
            response_content = openUrl(url)
            if response_content is None:
                logger.warning(f"Failed to get response from {url}, attempt {attempt + 1}/{RETRY_COUNT}")
            else:
                response = response_content.decode("utf8")
                # 正则表达式匹配svid
                reg = r'"id":"(.+?)",'
                pat = re.compile(reg)
                svid = re.findall(pat, response)
                if svid:
                    logger.info(f"Successfully extracted svid for {_lng}, {_lat}: {svid[0]}")
                    return svid[0]
                else:
                    logger.warning(f"No svid found in response for {_lng}, {_lat}, attempt {attempt + 1}/{RETRY_COUNT}")
        except Exception as e:
            logger.error(f"Error extracting svid for {_lng}, {_lat}, attempt {attempt + 1}/{RETRY_COUNT}: {str(e)}")

        if attempt < RETRY_COUNT - 1:
            logger.info(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

    logger.error(f"Failed to retrieve svid after {RETRY_COUNT} attempts for {_lng}, {_lat}")
    return None

# 定义速率限制参数
CALLS = 20
PERIOD = 1  # 秒

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def wgs2bd09mc(wgs_x, wgs_y):
    # 原始的函数体保持不变
    coords = f"{wgs_x},{wgs_y}"
    url = f'http://api.map.baidu.com/geoconv/v1/?coords={coords}&from=1&to=6&output=json&ak=x0toiDwXkm5GnrR10ZPA0bGBNTpGPKMY'
    res_content = openUrl(url)
    if res_content is None:
        logger.error(f"Failed to convert coordinates for {wgs_x}, {wgs_y}")
        return None, None
    else:
        res = res_content.decode()
        temp = json.loads(res)
        bd09mc_x = 0
        bd09mc_y = 0
        if temp['status'] == 0:
            bd09mc_x = temp['result'][0]['x']
            bd09mc_y = temp['result'][0]['y']
        else:
            logger.error(f"Error in coordinate conversion response: {temp}")
            return None, None
        return bd09mc_x, bd09mc_y

def stitch_images_opencv(image_paths, save_path, fid, error_file):
    # 读取图像
    images = []
    for path in image_paths:
        img = cv2.imread(path)
        if img is None:
            logger.error(f"Failed to load image: {path}")
            return False
        logger.info(f"Loaded image: {path}, shape: {img.shape}")
        images.append(img)

    # 检查所有图像的尺寸是否一致
    img_shapes = [img.shape for img in images]
    if len(set(img_shapes)) > 1:
        logger.error(f"Images have different sizes and cannot be stitched: {img_shapes}")
        return False

    # 检查图像数量是否少于8张
    if len(images) < 8:
        logger.warning(f"Only {len(images)} images available for stitching for FID {fid}")
        # 记录到错误文件中
        write_csv(error_file, [[fid, len(images)]], head=None, mode='a')

    try:
        # 尝试使用 OpenCL 进行拼接
        cv2.ocl.setUseOpenCL(True)
        logger.info("Using OpenCL for stitching")

        # 初始化 OpenCV 的拼接器
        stitcher = cv2.Stitcher_create()  # OpenCV 4.x 使用

        # 执行拼接
        status, stitched = stitcher.stitch(images)

        if status == cv2.Stitcher_OK:
            # 拼接成功，保存图片
            cv2.imwrite(save_path, stitched)
            logger.info(f"Panorama saved to {save_path}")
            return True
        else:
            logger.error(f"Error during stitching with OpenCL: {status}")
            raise cv2.error("Stitching failed with OpenCL")

    except cv2.error as e:
        # 如果OpenCL失败，切换到CPU处理
        logger.error(f"OpenCL failed, switching to CPU: {str(e)}")
        cv2.ocl.setUseOpenCL(False)

        # 重新初始化拼接器，使用 CPU
        stitcher = cv2.Stitcher_create()
        status, stitched = stitcher.stitch(images)

        if status == cv2.Stitcher_OK:
            # 拼接成功，保存图片
            cv2.imwrite(save_path, stitched)
            logger.info(f"Panorama saved to {save_path} using CPU")
            return True
        else:
            logger.error(f"Error during stitching with CPU: {status}")
            if status == cv2.Stitcher_ERR_NEED_MORE_IMGS:
                logger.error("Not enough images for stitching.")
            elif status == cv2.Stitcher_ERR_HOMOGRAPHY_EST_FAIL:
                logger.error("Homography estimation failed.")
            elif status == cv2.Stitcher_ERR_CAMERA_PARAMS_ADJUST_FAIL:
                logger.error("Camera parameter adjustment failed.")
            return False

if __name__ == "__main__":
    root = r'.\dir'
    read_fn = r'target.csv'
    error_fn = r'error.csv'
    incomplete_fn = r'incomplete_pano_records.csv'  # 保存未完全拼接的文件
    dir = r'images'
    panorama_dir = r'panoramas'  # 新建的用于保存全景图的文件夹
    if not os.path.exists(os.path.join(root, panorama_dir)):
        os.makedirs(os.path.join(root, panorama_dir))

    filenames_exist = glob.glob1(os.path.join(root, dir), "*.png")

    # 读取 csv 文件
    data = read_csv(os.path.join(root, read_fn))
    # 记录 header
    header = data[0]
    # 去掉 header
    data = data[1:]
    # 定义表头
    error_header = ['FID', 'WGS_X', 'WGS_Y', 'Error']

    # 错误文件路径
    error_file_path = os.path.join(root, error_fn)
    if not os.path.exists(error_file_path):
        # 如果错误文件不存在，创建并写入表头
        write_csv(error_file_path, [], head=error_header, mode='w')
        failed_fids = set()
    else:
        # 如果错误文件存在，读取已记录的错误 FID
        error_data = read_csv(error_file_path)
        error_data = error_data[1:]  # 去掉表头
        failed_fids = set(row[0] for row in error_data)
        logger.info(f"Loaded {len(failed_fids)} failed FIDs from error file.")

    # 在程序开始时，创建未完全拼接的错误文件并写入表头
    incomplete_file_path = os.path.join(root, incomplete_fn)
    if not os.path.exists(incomplete_file_path):
        write_csv(incomplete_file_path, [], head=["FID", "Image Count"], mode='w')

    headings = ['0', '45', '90', '135', '180', '225', '270', '315']  # 改为每45度一张

    count = 1

    for i in range(len(data)):
        fid = data[i][0]  # 使用第一列的FID作为唯一标识符

        # 检查当前 FID 是否在已知失败的 FID 集合中
        if fid in failed_fids:
            logger.info(f"FID {fid} is in failed FIDs list, skipping.")
            continue

        logger.info('Processing No. {} point...'.format(i + 1))

        # Check if panorama already exists
        panorama_save_path = os.path.join(root, panorama_dir, f'{fid}.png')
        if os.path.exists(panorama_save_path):
            logger.info(f"Panorama already exists for point {fid}, skipping.")
            continue  # Skip to the next iteration without calling coordinate conversion and SVID retrieval

        # Now, proceed to get wgs_x, wgs_y
        try:
            wgs_x, wgs_y = data[i][17], data[i][18]
        except IndexError:
            logger.error(f"Data index out of range for point {fid}")
            error_row = [fid, 'N/A', 'N/A', 'Data index out of range']
            write_csv(error_file_path, [error_row], head=None, mode='a')
            failed_fids.add(fid)
            continue

        # Proceed with coordinate conversion
        bd09mc_x, bd09mc_y = wgs2bd09mc(wgs_x, wgs_y)
        if bd09mc_x is None or bd09mc_y is None:
            logger.error(f"Coordinate conversion failed for point {fid}")
            error_row = [fid, wgs_x, wgs_y, 'Coordinate conversion failed']
            write_csv(error_file_path, [error_row], head=None, mode='a')
            failed_fids.add(fid)
            continue

        # Get SVID
        svid = getPanoId_with_retry(bd09mc_x, bd09mc_y)
        if svid:
            logger.info(f"SVID for point {fid}: {svid}")
        else:
            logger.error(f"SVID not found for point {fid}, skipping this point.")
            error_row = [fid, wgs_x, wgs_y, 'SVID not found']
            write_csv(error_file_path, [error_row], head=None, mode='a')
            failed_fids.add(fid)
            continue

        flag = True
        img_paths = []  # 保存图片的路径
        for k in range(len(headings)):
            img_name = "%s_%s_%s_%s.png" % (fid, str(wgs_x), str(wgs_y), headings[k])
            img_paths.append(os.path.join(root, dir, img_name))
            if not os.path.exists(img_paths[-1]):
                logger.debug(f"File not found: {img_paths[-1]}")
            flag = flag and os.path.exists(img_paths[-1])

        # 如果所有图片都存在，跳过下载
        if flag:
            logger.info(f"All images for point {fid} exist, skipping download.")
        else:
            # 带有重试机制的下载图片
            for h in range(len(headings)):
                save_fn = os.path.join(root, dir, '%s_%s_%s_%s.png' % (fid, str(wgs_x), str(wgs_y), headings[h]))
                url = 'https://mapsv0.bdimg.com/?qt=pr3d&fovy=90&quality=100&panoid={}&heading={}&pitch=0&width=1024&height=512'.format(
                    svid, headings[h]
                )
                img = grab_img_baidu_with_retry(url)
                if img is None:
                    logger.error(f"Failed to download image for heading {headings[h]} at point {fid} after {RETRY_COUNT} attempts")
                    error_row = [fid, wgs_x, wgs_y, f'Failed to download image at heading {headings[h]}']
                    write_csv(error_file_path, [error_row], head=None, mode='a')
                    failed_fids.add(fid)
                    continue
                else:
                    with open(save_fn, "wb") as f:
                        f.write(img)
                    logger.info(f"Image saved: {save_fn}")
                # 下载每张图片后添加延迟
                time.sleep(1)

        # 无论图片数量是否达到8张，都尝试进行拼接
        available_images = [p for p in img_paths if os.path.exists(p)]
        if len(available_images) == 8:
            logger.info(f"Attempting to stitch {len(available_images)} images for point {fid}.")
            try:
                img_paths_sorted = sorted(available_images, key=lambda x: headings.index(
                    os.path.splitext(os.path.basename(x).split('_')[3])[0]))
            except Exception as e:
                logger.error(f"Error sorting images for point {fid}: {str(e)}")
                error_row = [fid, wgs_x, wgs_y, 'Error sorting images']
                write_csv(error_file_path, [error_row], head=None, mode='a')
                failed_fids.add(fid)
                continue

            # 使用 stitch_images_opencv 并传入 FID 和记录文件
            success = stitch_images_opencv(img_paths_sorted, panorama_save_path, fid, incomplete_file_path)
            if not success:
                logger.error(f"Failed to stitch images for point {fid}.")
                error_row = [fid, wgs_x, wgs_y, 'Failed to stitch images']
                write_csv(error_file_path, [error_row], head=None, mode='a')
                failed_fids.add(fid)
        else:
            logger.error(f"No images available for point {fid}, skipping stitching.")
            error_row = [fid, wgs_x, wgs_y, 'No images available for stitching']
            write_csv(error_file_path, [error_row], head=None, mode='a')
            failed_fids.add(fid)

        # 记得睡眠6秒，太快可能会被封
        time.sleep(6)
        count += 1
