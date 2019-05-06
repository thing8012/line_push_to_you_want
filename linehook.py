from flask import Flask, request, abort, send_file

from FileOperate import file_operate
import config.read_config
import dbOperate.dbOperate
import dateutil.parser as dp
import datetime
import pytz
import re
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError
)
from linebot.models import *

app = Flask(__name__)
# 取得自己的config
sys_config = config.read_config.SystemConfig("config.yaml")
sys_config = sys_config.get_config()

# 初始化line的驗證資訊
line_bot_api = LineBotApi(sys_config['token'])
handler = WebhookHandler(sys_config['channel_secret'])

# 初始化db連線資訊
db_conn = dbOperate.dbOperate.dbOperate(mongo_str=sys_config["mongodb"]["host"])

# 等候時間 (分鐘)
wait_minute = 20


@app.route('/hello', methods=['get'])
def hello_world():
    """
        測試heroku中程式是否活著
        :return:
    """
    return 'Hello, World! test'


@app.route('/images/<image_name>')
def get_image(image_name):
    """
        Serves return image.
    """
    # 依照圖片名稱搜尋圖片出來
    res = file_operate.get_file(db_conn, image_name)
    return send_file(res["image_file"], attachment_filename=res["image_name"], mimetype=res["mime_type"])


@app.route('/api/v1.0/send_message', methods=['POST'])
def send_message():
    """
        直接使用資料庫中的user id做訊息傳送
        :return:
    """
    # 解析request
    body = dict(request.form)
    image = request.files["picture"]
    # 將檔案儲存起來，並反回URL
    img_url = file_operate.save_file(db_conn, image)
    # 將傳送過來的時間轉成時間格式
    tz = pytz.timezone('Asia/Taipei')  # 設定時區
    start = dp.parse(body["time"])
    start = tz.localize(start)
    # 計算判斷到輪椅的時間及最大時間(加上等候的時間)
    end = start + datetime.timedelta(minutes=wait_minute)
    # 搜尋時段內即將到達的所有公車車排
    condition = {"StopID": str(body["stop_id"]), "NextBusTime": {"$gte": start, "$lte": end}}
    # 公車列表清單
    plate = db_conn.db_find("Bus", "bus_route", condition)

    for bus in plate:
        user_id = db_conn.db_find("Bus", "memberList", {"bus_plate": bus["PlateNumb"]})
        for u_id in user_id:
            # 傳送圖片訊息
            image_message = ImageSendMessage(
                original_content_url=img_url,
                preview_image_url=img_url
            )
            line_bot_api.push_message(u_id["userId"], image_message)
            # 傳送文字訊息
            message = TextSendMessage(text="司機【" + u_id["userName"] + "】\n\n請注意，於【" +
                                           bus["StopName"]["Zh_tw"] + "】站\n\n" +
                                           "有乘客需服務。謝謝" + u"\U00100001\U00100001\U00100001")
            line_bot_api.push_message(u_id["userId"], message)
    return "ok"


@app.route("/api/v1.0/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    # print("body:",body)
    app.logger.info("Request body: " + body)

    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return 'ok'


def validate_plate(plate):
    """
        判斷輸入是否為車牌格式
        :param plate: 輸入字串
        :return:
    """
    if len(plate) == 6:
        if re.match(r"^[0-9]*\-[A-Z].", plate) is not None:
            return True
    return False


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    """
        主要處理user權限
        :param event:
        :return:
    """
    user_id = event.source.user_id
    profile = line_bot_api.get_profile(user_id)
    user_list = db_conn.db_find("Bus", "memberList", {"userId": user_id})
    message = ""
    if not len(user_list):
        if event.message.text.upper() == "HI":
            message = TextSendMessage(text="【" + profile.display_name + "】您好，接下來請輸入您的行駛公車的車牌號碼。")
        else:
            if validate_plate(event.message.text.upper()):
                # 判斷輸入的內容是否包含英數字
                db_conn.db_insert("Bus", "memberList", {"userId": user_id, "userName": profile.display_name,
                                                        "bus_plate": event.message.text.upper()})
                message = \
                    TextSendMessage(text="您好【" + profile.display_name + "】\n" + "您的車牌號碼為【" +
                                         event.message.text.upper()+"】，以為您註冊。")
            else:
                message = \
                    TextSendMessage(text="您好，可以跟我們說聲【HI】，讓我們認識您， 謝謝~" + u"\U00100001\U00100001\U00100001")

    line_bot_api.push_message(user_id, message)
    # line_bot_api.reply_message(event.reply_token, TextSendMessage(text=response_text))


if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, port=80, debug=False)
