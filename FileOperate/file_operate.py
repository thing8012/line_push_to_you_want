from bson import Binary
from io import StringIO
from io import BytesIO
import time
import config.read_config

# 取得自己的config
sys_config = config.read_config.SystemConfig("config.yaml")
sys_config = sys_config.get_config()


def save_file(db_conn, f):
    # 取得時間序當做檔案名稱(較不易重複)
    timestamp = str(int(time.time()))
    sub_filename = f.filename.split('.')[-1]
    # 組裝要給line的url
    image_url = sys_config["heroku_url"] + "images/" + timestamp + "." + sub_filename
    # 組裝要存在db的資料
    insert_data = {
        "image_file": Binary(f.read()),
        "image_name": timestamp + "." + sub_filename,
        "mime_type": f.content_type
    }
    # 將檔案存入db中
    db_conn.db_insert("Bus", "pictureList", insert_data)
    # 返回圖片的URL
    return image_url


def get_file(db_conn, image_name):
    image = db_conn.db_find("Bus", "pictureList", {"image_name": image_name})
    response_data = {
        "image_file": BytesIO(image[0]["image_file"]),
        "mime_type": image[0]["mime_type"],
        "image_name": image[0]["image_name"]
    }
    return response_data
