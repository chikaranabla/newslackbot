import json
import traceback
from typing import Mapping, Dict, Tuple # Tupleを追加
from werkzeug import Request, Response
from dify_plugin import Endpoint
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging
import re

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Dify Markdown to Slack mrkdwn Conversion Function ---
# (変更なし、前の回答のものをそのまま使用してください)
def convert_markdown_to_slack(dify_text: str) -> str:
    """
    Dify Markdown → Slack mrkdwn に変換する関数。
    (元のコードと同じ)
    """
    text = dify_text

    # 0. コードブロック (``` ... ```) を保護
    codeblocks = {}
    def protect_codeblock(m):
        idx = len(codeblocks)
        placeholder = f"%%CODEBLOCK_{idx}%%"
        codeblocks[placeholder] = m.group(0)  # ```...```
        return placeholder
    text = re.sub(r"```([\s\S]*?)```", protect_codeblock, text)

    # 1. インラインコード `code` を保護
    inlinecodes = {}
    def protect_inlinecode(m):
        idx = len(inlinecodes)
        placeholder = f"%%INLINECODE_{idx}%%"
        inlinecodes[placeholder] = m.group(0)  # `code`
        return placeholder
    text = re.sub(r"`([^`]+)`", protect_inlinecode, text)

    # 2. リンク [title](url) → <url|title>
    text = re.sub(r"\[([^\]]+)\]\((http[^\)]+)\)", r"<\2|\1>", text)

    # 3. 見出し (#, ##, ###...) をプレースホルダ化
    heading_map = {}
    def protect_heading(m):
        idx = len(heading_map)
        placeholder = f"%%HEADING_{idx}%%"
        heading_text = m.group(2).strip()  # 実際の見出し内容
        heading_map[placeholder] = heading_text
        return placeholder

    # 行頭に # が 1～6個あるものを抽出
    text = re.sub(
        r'^[ \t]*(#{1,6})\s+(.*)$',
        protect_heading,
        text,
        flags=re.MULTILINE
    )

    # 4. 行頭に '*　' (全角スペース) → '* '（半角スペース1つ）
    #    または複数半角スペースも '* ' に統一
    text = re.sub(r'^[ \t]*\*[\u3000\s]+', '* ', text, flags=re.MULTILINE)
    text = re.sub(r'^[ \t]*[・•]\s+', '* ', text, flags=re.MULTILINE)

    # 5. 太字 (**text** / __text__) をプレースホルダ化
    bold_map = {}
    def protect_bold(m):
        idx = len(bold_map)
        placeholder = f"%%BOLD_{idx}%%"
        bold_map[placeholder] = m.group(1)
        return placeholder
    text = re.sub(r"\*\*(.+?)\*\*", protect_bold, text)
    text = re.sub(r"__(.+?)__", protect_bold, text)

    # 6. 取り消し線 ~~text~~ → ~text~
    text = re.sub(r"~~(.+?)~~", r"~\1~", text)

    # 7. 斜体 (*text*) → _text_ (箇条書きマーカーの * は除外)
    lines = text.splitlines()
    new_lines = []
    italic_pattern = r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)'
    for line in lines:
        m = re.match(r'^(\s*\*\s)(.*)$', line)
        if m:
            prefix, rest = m.groups()
            rest_converted = re.sub(italic_pattern, r'_\1_', rest)
            new_lines.append(prefix + rest_converted)
        else:
            new_lines.append(re.sub(italic_pattern, r'_\1_', line))
    text = "\n".join(new_lines)

    # 8. プレースホルダ: 見出し → Slack 太字 (*heading_text*)
    for placeholder, heading_text in heading_map.items():
        text = text.replace(placeholder, f"*{heading_text}*")

    # 9. プレースホルダ: 太字 → *text*
    for placeholder, content in bold_map.items():
        text = text.replace(placeholder, f"*{content}*")

    # 10. コードブロック / インラインコード を元に戻す
    for placeholder, block_text in codeblocks.items():
        text = text.replace(placeholder, block_text)
    for placeholder, inline_text in inlinecodes.items():
        text = text.replace(placeholder, inline_text)

    return text
# --- Conversation Store (In-Memory Example) ---
# 注意: この辞書はサーバープロセスが終了すると内容が失われます。
# 永続化が必要な場合は、Redisやデータベースなどに変更してください。
conversation_store: Dict[str, str] = {}

class NewSlackBotEndpoint(Endpoint):

    def get_conversation_key_and_reply_ts(self, event: Dict) -> Tuple[str | None, str | None]:
        """
        Slackイベントから会話IDを特定するためのキーと、返信先のスレッドTSを取得します。
        `app_mention` イベントと DM (`message`, channel_type='im') を主に想定。

        Returns:
            tuple[str | None, str | None]: (conversation_key, reply_thread_ts)
            - conversation_key: スレッドなら起点メッセージのts, DMならchannel_id。特定不可ならNone。
            - reply_thread_ts: 返信先のthread_ts。DMならNone。チャンネルの新規メンションなら起点ts。
        """
        channel_type = event.get("channel_type")
        channel_id = event.get("channel")
        ts = event.get("ts") # 現在のメッセージのタイムスタンプ
        thread_ts = event.get("thread_ts") # スレッドの起点メッセージのタイムスタンプ (あれば)
        event_type = event.get("type")

        key = None
        reply_ts = None

        if channel_type == "im" and event_type == "message":
            # DMの場合: チャンネルIDがキー。スレッド返信はしない。
            key = channel_id
            reply_ts = None
            logger.debug(f"[ConvKey] DM conversation. Key: {key}, Reply TS: {reply_ts}")
        elif event_type == "app_mention":
            # チャンネルでのメンション (新規 or スレッド内)
            if thread_ts:
                # スレッド内メンション: スレッドの起点TSがキー。返信も同じスレッドへ。
                key = thread_ts
                reply_ts = thread_ts
                logger.debug(f"[ConvKey] Thread Mention conversation. Key: {key}, Reply TS: {reply_ts}")
            else:
                # 新規メンション: 現在のメッセージTSがキー(これがスレッドの起点になる)。返信はこのメッセージへのスレッド。
                key = ts
                reply_ts = ts # このメッセージに対してスレッドを開始する
                logger.debug(f"[ConvKey] New Channel Mention conversation. Key: {key}, Reply TS: {reply_ts}")
        # elif event_type == "message" and thread_ts and channel_type != "im":
            # スレッド内の通常メッセージ(メンションなし)はここで処理しない
            # logger.debug(f"[ConvKey] Ignoring non-mention message in thread {thread_ts}")
            # pass # 何もしない
        else:
            # その他の予期しないケース (DM以外でのmessageイベントなど)
            logger.warning(f"[ConvKey] Could not determine key for event: Type={event_type}, ChannelType={channel_type}, HasThreadTS={bool(thread_ts)}")

        return key, reply_ts

    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        # --- Retry logic and initial data parsing (変更なし) ---
        logger.debug(f"Received request: Method={r.method}, Path={r.path}")
        # logger.debug(f"Request headers: {r.headers}") # 必要ならコメント解除
        try:
            raw_data = r.get_data(as_text=True)
            # logger.debug(f"Raw request body: {raw_data}") # 必要ならコメント解除
        except Exception as e:
            logger.error(f"Failed to get raw request body: {e}", exc_info=True)
            raw_data = None

        retry_num = r.headers.get("X-Slack-Retry-Num")
        allow_retry = settings.get("allow_retry", False)
        logger.debug(f"Allow Retry setting: {allow_retry}")
        if not allow_retry and (r.headers.get("X-Slack-Retry-Reason") == "http_timeout" or ((retry_num is not None and int(retry_num) > 0))):
            logger.info("Ignoring Slack retry request based on headers.")
            return Response(status=200, response="ok, retry ignored")

        data = None
        try:
            if raw_data and raw_data.strip():
                data = json.loads(raw_data)
            else:
                logger.warning("Request body is empty.")
                return Response(status=200, response="ok, empty body")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}", exc_info=True)
            return Response(status=400, response="Bad Request: Invalid JSON.")
        except Exception as e:
            logger.error(f"Unexpected error during JSON parsing: {e}", exc_info=True)
            return Response(status=500, response="Internal Server Error during JSON parsing.")

        if not isinstance(data, dict):
            logger.warning(f"Parsed data is not a dictionary. Type: {type(data)}, Data: {data}")
            return Response(status=400, response="Bad Request: Expected JSON object.")

        # logger.info(f"Parsed Slack event data: {json.dumps(data, indent=2, ensure_ascii=False)}") # 必要ならコメント解除

        request_type = data.get("type")

        # --- Handle URL verification (変更なし) ---
        if request_type == "url_verification":
            challenge_code = data.get("challenge")
            if challenge_code:
                logger.info(f"Handling URL verification, challenge code: {challenge_code}")
                return Response(
                    response=json.dumps({"challenge": challenge_code}),
                    status=200,
                    content_type="application/json"
                )
            else:
                logger.warning("URL verification request received without challenge code.")
                return Response(status=400, response="Bad Request: Missing challenge code.")

        # --- Handle event callbacks ---
        elif request_type == "event_callback":
            event = data.get("event")
            if not event or not isinstance(event, dict):
                logger.warning("No 'event' field or invalid format in event_callback.")
                return Response(status=200, response="ok, invalid event_callback format")

            # --- Ignore messages from bots or specific subtypes (変更なし) ---
            if event.get("bot_id") is not None:
                logger.info("Ignoring event from bot (bot_id present).")
                return Response(status=200, response="ok, ignored bot message")

            # 無視するサブタイプを定義
            ignored_subtypes = ["message_deleted", "message_changed", "channel_join", "channel_leave", "thread_broadcast"]
            if event.get("subtype") is not None and event.get("subtype") in ignored_subtypes:
                logger.info(f"Ignoring event with subtype: {event.get('subtype')}")
                return Response(status=200, response="ok, ignored subtype")
            # --- End Ignore ---

            # --- Extract event details ---
            event_type = event.get("type")
            channel_id = event.get("channel")
            user_id = event.get("user")
            message_text = event.get("text", "")
            ts = event.get("ts") # メッセージ自体のタイムスタンプ

            # 必須フィールドチェック
            if not all([channel_id, user_id, ts]):
                logger.warning(f"Missing essential fields (channel, user, ts) in event: {event}")
                return Response(status=200, response="ok, missing essential fields")

            # --- Get Conversation Key and Reply Target ---
            # このイベントが処理対象か、どの会話に属するか、どこに返信すべきかを判断
            conversation_key, reply_thread_ts = self.get_conversation_key_and_reply_ts(event)

            query_text = None # Difyに送るテキスト
            should_process = False # Difyを呼び出すべきかどうかのフラグ

            # --- Determine if we should process this event and extract query ---
            # conversation_key が None でない場合、処理対象のイベントタイプと判断
            if conversation_key:
                # 1. app_mention Event
                if event_type == "app_mention":
                    logger.info(f"Processing app_mention from user {user_id} in {channel_id} (ts={ts}, thread_ts={event.get('thread_ts')})")
                    raw_message = message_text.strip()
                    # Botメンション部分を除去 (例: "<@Uxxxx> query text" -> "query text")
                    query_text_match = re.sub(r"<@U[A-Z0-9]+>\s*", "", raw_message, count=1).strip()
                    if query_text_match: # メンション後にテキストがある場合のみ処理
                        query_text = query_text_match
                        should_process = True
                        logger.info(f"Extracted query from app_mention: '{query_text}'")
                    else:
                        # メンションのみの場合は処理しない (必要ならヘルプメッセージを返す)
                        logger.info("app_mention contains only mention, skipping Dify.")
                        # 例: client.chat_postMessage(channel=channel_id, text="はい、私です。何かお手伝いできることはありますか？", thread_ts=reply_thread_ts)
                        # return Response(status=200, response="ok, mention only")

                # 2. message Event (Direct Message)
                elif event_type == "message" and event.get("channel_type") == "im":
                    logger.info(f"Processing direct message (DM) from user {user_id} in {channel_id} (ts={ts})")
                    query_text = message_text.strip()
                    if query_text: # 空メッセージは無視
                        should_process = True
                        logger.info(f"Extracted query from DM: '{query_text}'")
                    else:
                        logger.info("Empty DM received, skipping Dify.")

            # --- Dify Invocation Logic ---
            if should_process and query_text:
                dify_app_config = settings.get("app")
                bot_token = settings.get("bot_token")

                # --- Check essential configurations (変更なし) ---
                if not bot_token or not dify_app_config or not isinstance(dify_app_config, dict) or not dify_app_config.get("app_id"):
                    error_message = "エラー: Slack Bot TokenまたはDify App設定が不十分です。管理者に連絡してください。"
                    logger.error(error_message + f" Token:{'OK' if bot_token else 'NG'}, Config:{'OK' if dify_app_config else 'NG'}")
                    if bot_token and channel_id:
                        try:
                            client = WebClient(token=bot_token)
                            client.chat_postMessage(channel=channel_id, text=error_message, thread_ts=reply_thread_ts)
                        except Exception as slack_e:
                            logger.error(f"Could not notify user about config error: {slack_e}", exc_info=True)
                    return Response(status=500, response=error_message)

                dify_app_id = dify_app_config["app_id"]
                user_id_for_dify = f"slack-{user_id}" # Difyに渡すユーザー識別子

                # --- Get existing Conversation ID (変更なし) ---
                existing_conversation_id = None
                if conversation_key: # Ensure key is not None before accessing store
                    existing_conversation_id = conversation_store.get(conversation_key)
                    log_msg = f"Found existing Dify conversation_id '{existing_conversation_id}'" if existing_conversation_id else "No existing Dify conversation_id found"
                    logger.info(f"{log_msg} for key '{conversation_key}'")
                # else: # conversation_keyがNoneの場合は Dify 呼び出しに進まないはず

                try:
                    # --- Prepare Dify API Request (変更なし) ---
                    dify_request_params = {
                        "app_id": dify_app_id,
                        "query": query_text,
                        "inputs": {}, # 必要に応じて設定
                        "response_mode": "blocking", # blockingモードを使用
                    }
                    if existing_conversation_id:
                        dify_request_params["conversation_id"] = existing_conversation_id

                    logger.info(f"Invoking Dify app '{dify_app_id}' for user '{user_id_for_dify}' (Conv ID: {existing_conversation_id})")
                    # logger.debug(f"Dify request params: {dify_request_params}") # 必要ならコメント解除

                    # --- Call Dify API (変更なし) ---
                    # chat.invokeがconversation_idを扱うことを確認済み
                    response_from_dify = self.session.app.chat.invoke(**dify_request_params)
                    # --- End Dify API Call ---

                    # logger.debug(f"Raw response from Dify: {response_from_dify}") # 必要ならコメント解除

                    # --- Process Dify Response (変更なし) ---
                    dify_answer = response_from_dify.get("answer")
                    new_conversation_id = response_from_dify.get("conversation_id") # blockingモードのレスポンスに含まれることを確認済み

                    if not dify_answer:
                         logger.warning(f"Dify response missing 'answer'. Response: {response_from_dify}")
                         dify_answer = "(エラー: Difyから有効な応答がありませんでした)" # デフォルトエラーメッセージ

                    if not new_conversation_id:
                         logger.warning(f"Dify response missing 'conversation_id'. Response: {response_from_dify}")
                    # --- End Process Dify Response ---

                    # --- Store/Update conversation_id (変更なし) ---
                    if new_conversation_id and conversation_key: # Ensure key is not None
                        if existing_conversation_id != new_conversation_id:
                             logger.info(f"Storing/Updating Dify conversation_id '{new_conversation_id}' for key '{conversation_key}'")
                             conversation_store[conversation_key] = new_conversation_id
                             # logger.debug(f"Current conversation store: {conversation_store}") # 必要ならコメント解除
                        else:
                             logger.debug(f"Conversation ID remained the same: {new_conversation_id}")
                    # elif not conversation_key: # keyがNoneならここに到達しないはず
                    #      logger.warning("Cannot store conversation_id because conversation key is None.")
                    # --- End Store ---

                    logger.info(f"Received answer from Dify (first 200 chars): {dify_answer[:200]}...")

                    try:
                        # --- Send Reply to Slack (変更なし) ---
                        client = WebClient(token=bot_token)
                        slack_mrkdwn_text = convert_markdown_to_slack(dify_answer)
                        # logger.debug(f"Converted Slack mrkdwn (first 200 chars): {slack_mrkdwn_text[:200]}...") # 必要ならコメント解除

                        result = client.chat_postMessage(
                            channel=channel_id,
                            text=slack_mrkdwn_text,
                            thread_ts=reply_thread_ts, # スレッドに返信 (DMの場合はNone)
                            mrkdwn=True
                        )
                        logger.info(f"Posted message to Slack channel {channel_id} (thread: {reply_thread_ts}) ts: {result.get('ts')}")
                        # --- End Reply ---

                        return Response(
                            status=200,
                            response=json.dumps({"status": "success", "message_ts": result.get('ts')}, ensure_ascii=False),
                            content_type="application/json"
                        )
                    except SlackApiError as e:
                        error_detail = f"Slack API Error posting message: {e.response['error']}"
                        logger.error(error_detail, exc_info=True)
                        # Dify処理成功、Slack投稿失敗
                        return Response(status=500, response=json.dumps({"status": "error", "message": error_detail}), content_type="application/json")

                except Exception as e: # Dify API呼び出しや応答処理中のエラー
                    error_trace = traceback.format_exc()
                    logger.error(f"Error during Dify interaction or processing:\n{error_trace}")
                    # ユーザーにエラー通知試行
                    try:
                        client = WebClient(token=bot_token)
                        error_message_to_user = f"すみません、処理中にエラーが発生しました。時間をおいて再試行するか、管理者に連絡してください。(Ref: {ts})"
                        client.chat_postMessage(channel=channel_id, text=error_message_to_user, thread_ts=reply_thread_ts)
                    except Exception as slack_e:
                        logger.error(f"Could not notify user about Dify invocation error: {slack_e}", exc_info=True)
                    # サーバーエラー応答
                    return Response(
                        status=500,
                        response=json.dumps({"status": "error", "message": "Internal Server Error during Dify interaction", "detail": str(e)}),
                        content_type="application/json",
                    )
            else:
                # 処理対象外のイベント、またはメンションのみでテキストがない場合
                logger.info(f"Event skipped (should_process={should_process}, query_text='{query_text}').")
                return Response(status=200, response="ok, skipped")

        else: # event_callback, url_verification 以外のトップレベルリクエストタイプ
            logger.info(f"Ignoring top-level request type: {request_type}")
            return Response(status=200, response="ok, ignored top-level type")