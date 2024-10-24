import re, os
import json
import requests
import time, glob
import csv
import logging
from colorlog import ColoredFormatter
import cv2  # 使用 OpenCV 进行图像拼接
from datetime import datetime


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

# read csv
def write_csv(filepath, data, head=None):
    if head:
        data = [head] + data
    with open(filepath, mode='w', encoding='UTF-8-sig', newline='') as f:
        writer = csv.writer(f)
        for i in data:
            writer.writerow(i)

# write csv
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
    response = requests.get(_url, headers=headers)
    if response.status_code == 200:  # 如果状态码为200，说明服务器已成功处理了请求，则继续处理数据
        return response.content
    else:
        return None


def getPanoId(_lng, _lat):
    # 获取百度街景中的svid get svid of baidu streetview
    url = "https://mapsv0.bdimg.com/?&qt=qsdata&x=%s&y=%s&l=17.031000000000002&action=0&mode=day&t=1530956939770" % (
        str(_lng), str(_lat))
    response = openUrl(url).decode("utf8")
    if response is None:
        return None
    reg = r'"id":"(.+?)",'
    pat = re.compile(reg)
    try:
        svid = re.findall(pat, response)[0]
        return svid
    except Exception as e:
        logger.error(f"Error extracting svid: {str(e)}")
        return None


# 官方转换函数
# 因为百度街景获取时采用的是经过二次加密的百度墨卡托投影bd09mc (Change wgs84 to baidu09)
def wgs2bd09mc(wgs_x, wgs_y):
    # to:5是转为bd0911，6是转为百度墨卡托
    url = 'http://api.map.baidu.com/geoconv/v1/?coords={}+&from=1&to=6&output=json&ak={}'.format(
        wgs_x + ',' + wgs_y,
        'x0toiDwXkm5GnrR10ZPA0bGBNTpGPKMY'
    )
    res = openUrl(url).decode()
    temp = json.loads(res)
    bd09mc_x = 0
    bd09mc_y = 0
    if temp['status'] == 0:
        bd09mc_x = temp['result'][0]['x']
        bd09mc_y = temp['result'][0]['y']

    return bd09mc_x, bd09mc_y


def stitch_images_opencv(image_paths, save_path):
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
        # 如果拼接失败，输出错误状态
        logger.error(f"Error during stitching: {status}")
        if status == cv2.Stitcher_ERR_NEED_MORE_IMGS:
            logger.error("Not enough images for stitching.")
        elif status == cv2.Stitcher_ERR_HOMOGRAPHY_EST_FAIL:
            logger.error("Homography estimation failed.")
        elif status == cv2.Stitcher_ERR_CAMERA_PARAMS_ADJUST_FAIL:
            logger.error("Camera parameter adjustment failed.")
        return False


if __name__ == "__main__":
    root = r'.\dir'
    read_fn = r'point_coordinate_intersect.csv'
    error_fn = r'error_road_intersection.csv'
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
    # 记录爬取失败的图片
    error_img = []
    # 记录没有svid的位置
    svid_none = []
    headings = ['0', '45', '90', '135', '180', '225', '270', '315']  # 改为每45度一张
    pitchs = '0'

    count = 1
    for i in range(len(data)):
        logger.info('Processing No. {} point...'.format(i + 1))
        fid = data[i][0]  # 使用第一列的FID作为唯一标识符
        wgs_x, wgs_y = data[i][17], data[i][18]

        try:
            bd09mc_x, bd09mc_y = wgs2bd09mc(wgs_x, wgs_y)
        except Exception as e:
            logger.error(f"Coordinate conversion failed for point {fid}: {str(e)}")
            continue

        flag = True
        img_paths = []  # 保存图片的路径
        for k in range(len(headings)):
            img_name = "%s_%s_%s_%s.png" % (fid, str(wgs_x), str(wgs_y), headings[k])
            img_paths.append(os.path.join(root, dir, img_name))
            if not os.path.exists(img_paths[-1]):
                logger.debug(f"File not found: {img_paths[-1]}")
            flag = flag and os.path.exists(img_paths[-1])

        # 打印调试信息，检查是否所有路径和文件都存在
        logger.debug(f"Image paths: {img_paths}")
        logger.debug(f"Existence flags: {[os.path.exists(p) for p in img_paths]}")

        # 如果已经存在全景图，则跳过拼接
        panorama_save_path = os.path.join(root, panorama_dir, f'{fid}.png')
        if os.path.exists(panorama_save_path):
            logger.info(f"Panorama already exists for point {fid}, skipping.")
            continue

        # 如果所有图片都存在，跳过下载
        if flag:
            logger.info(f"All images for point {fid} exist, skipping download.")
        else:
            # 获取SVID
            svid = getPanoId(bd09mc_x, bd09mc_y)
            if svid:
                logger.info(f"SVID for point {fid}: {svid}")
            else:
                logger.error(f"SVID not found for point {fid}, skipping this point.")
                continue

            # 下载图片并保存，带有重试机制
            for h in range(len(headings)):
                save_fn = os.path.join(root, dir, '%s_%s_%s_%s.png' % (fid, str(wgs_x), str(wgs_y), headings[h]))
                url = 'https://mapsv0.bdimg.com/?qt=pr3d&fovy=90&quality=100&panoid={}&heading={}&pitch=0&width=1024&height=512'.format(
                    svid, headings[h]
                )
                img = grab_img_baidu_with_retry(url)
                if img is None:
                    logger.error(f"Failed to download image for heading {headings[h]} at point {fid} after {RETRY_COUNT} attempts")
                    error_img.append([fid, wgs_x, wgs_y, headings[h]])  # 记录失败的图片
                    continue
                else:
                    with open(save_fn, "wb") as f:
                        f.write(img)
                    logger.info(f"Image saved: {save_fn}")

        # 无论图片数量是否达到8张，都尝试进行拼接
        available_images = [p for p in img_paths if os.path.exists(p)]
        if len(available_images) > 0:
            logger.info(f"Attempting to stitch {len(available_images)} images for point {fid}.")
            try:
                img_paths_sorted = sorted(available_images, key=lambda x: headings.index(
                    os.path.splitext(os.path.basename(x).split('_')[3])[0]))
            except Exception as e:
                logger.error(f"Error sorting images for point {fid}: {str(e)}")
                continue

            success = stitch_images_opencv(img_paths_sorted, panorama_save_path)
            if not success:
                logger.error(f"Failed to stitch images for point {fid}.")
        else:
            logger.error(f"No images available for point {fid}, skipping stitching.")

        # 记得睡眠6s，太快可能会被封
        time.sleep(6)
        count += 1

    # 保存失败的图片
    if len(error_img) > 0:
        write_csv(os.path.join(root, error_fn), error_img, header)


