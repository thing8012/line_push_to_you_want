from flask import Flask, request, abort
import config.read_config
import dbOperate.dbOperate
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


@app.route('/hello', methods=['get'])
def hello_world():
    """
        測試heroku中程式是否活著
        :return:
    """
    return 'Hello, World!'


@app.route('/send_message', methods=['POST'])
def send_message():
    """
        直接使用資料庫中的user id做訊息傳送
        :return:
    """
    body = request.form
    user_list = db_conn.db_find("Bus", "memberList", {"role": body.get("role")})
    for user in user_list:
        line_bot_api.push_message(user["userId"], TextSendMessage(text=body.get("message")))
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
            # message = TextSendMessage(text="【" + profile.display_name + "】您好，請選擇您要注冊的身份類別\n1：司機\n2：使用者")
            message = TemplateSendMessage(
                alt_text='角色選擇',
                template=ButtonsTemplate(
                    title='角色選擇',
                    text="【" + profile.display_name + "】您好，請選擇您要注冊的身份類別",
                    actions=[
                        MessageTemplateAction(
                            label='司機',
                            text='driver'
                        ),
                        MessageTemplateAction(
                            label='使用者',
                            text='user'
                        )
                    ]
                )
            )
        elif event.message.text == "driver":
            db_conn.db_insert("Bus", "memberList", {"userId": user_id, "userName": profile.display_name,
                                                    "role": "driver"})
            message = TextSendMessage(text="【" + profile.display_name + "】您好，以替您註冊成【司機】權限")
        elif event.message.text == "user":
            db_conn.db_insert("Bus", "memberList", {"userId": user_id, "userName": profile.display_name,
                                                    "role": "user"})
            message = TextSendMessage(text="【" + profile.display_name + "】您好，以替您註冊成【使用者】權限")
        else:
            message = \
                TextSendMessage(text="您好，可以跟我們說聲【HI】，讓我們認識您， 謝謝~" + u"\U00100001\U00100001\U00100001")

    line_bot_api.push_message(user_id, message)
    # line_bot_api.reply_message(event.reply_token, TextSendMessage(text=response_text))


if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, port=80)
