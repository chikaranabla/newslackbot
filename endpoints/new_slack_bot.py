import json
import traceback
from typing import Mapping
from werkzeug import Request, Response
from dify_plugin import Endpoint
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging
import re

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

## CHANGED SECTION: Dify の Markdown を Slack 書式 (mrkdwn) に変換する関数
def convert_markdown_to_slack(dify_text: str) -> str:
    """
    Dify Markdown → Slack mrkdwn に変換する関数。
    
    主なポイント:
      - 見出し (#, ##, ###...) は一時的にプレースホルダ化してから Slack 太字 (*見出し*) にする。
        → そうしないと後で斜体変換に巻き込まれてしまう可能性がある。
      - 行頭の '*　' (アスタリスク+全角スペース) や '*   ' (複数半角スペース) を必ず '* ' (半角スペース1つ) に統一。
      - '**text**' / '__text__' はプレースホルダ化 → 後で '*text*' に復元。
      - 斜体 (*text*) は箇条書きマーカーとは区別して変換。
      - コードブロックやインラインコードはプレースホルダ化 → 復元。
      - リンク [title](url) → <url|title>
      - 取り消し線 ~~text~~ → ~text~
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
    #   例: "*    " → "* "
    #        "*　"  → "* " (全角スペース)
    text = re.sub(r'^[ \t]*\*[\u3000\s]+', '* ', text, flags=re.MULTILINE)

    # 行頭に「・」「•」があれば '* ' に置換
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
        # 行頭に "* " があれば取り除いた後に斜体変換 (衝突回避)
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

class NewSlackBotEndpoint(Endpoint):
    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        logger.debug(f"Received request: Method={r.method}, Path={r.path}")
        logger.debug(f"Request headers: {r.headers}")

        try:
            raw_data = r.get_data(as_text=True)
            logger.debug(f"Raw request body: {raw_data}")
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

        logger.info(f"Parsed Slack event data: {json.dumps(data, indent=2, ensure_ascii=False)}")

        request_type = data.get("type")

        # --- Handle different request types ---
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

        elif request_type == "event_callback":
            event = data.get("event")
            if not event or not isinstance(event, dict):
                logger.warning("No 'event' field or invalid format in event_callback.")
                return Response(status=200, response="ok, invalid event_callback format")

            # --- Ignore messages from bots or specific subtypes ---
            if event.get("bot_id") is not None:
                logger.info("Ignoring event from bot itself (bot_id present).")
                return Response(status=200, response="ok, ignored bot message")

            if event.get("subtype") is not None and event.get("subtype") not in ["thread_broadcast"]:
                logger.info(f"Ignoring event with subtype: {event.get('subtype')}")
                return Response(status=200, response="ok, ignored subtype")
            # --- End Ignore ---

            event_type = event.get("type")
            channel_type = event.get("channel_type")
            logger.info(f"Processing event_callback. Event type: {event_type}, Channel Type: {channel_type}")

            message_text = event.get("text", "")
            user_id = event.get("user")
            channel_id = event.get("channel")
            ts = event.get("ts")
            thread_ts = event.get("thread_ts", ts)
            query_text = None

            if not all([user_id, channel_id, ts, message_text is not None]):
                 logger.warning(f"Missing essential fields (user, channel, ts, text) in event: {event}")
                 return Response(status=200, response="ok, missing essential fields")

            if event_type == "app_mention":
                logger.info(f"Processing app_mention in channel {channel_id} from user {user_id}.")
                raw_message = message_text.strip()
                query_text_match = re.sub(r"<@\w+>\s*", "", raw_message, count=1)
                if query_text_match != raw_message:
                     query_text = query_text_match.strip()
                     logger.info(f"Extracted query from app_mention: '{query_text}'")
                elif raw_message.startswith("<@"):
                     parts = raw_message.split(">", 1)
                     if len(parts) > 1: query_text = parts[1].strip()
                     else: query_text = ""
                     logger.info(f"Extracted query from app_mention (fallback): '{query_text}'")

            elif event_type == "message" and channel_type == "im":
                logger.info(f"Processing direct message (DM) in channel {channel_id} from user {user_id}.")
                query_text = message_text.strip()
                logger.info(f"Extracted query from DM: '{query_text}'")

            if query_text is not None:
                dify_app_config = settings.get("app")
                bot_token = settings.get("bot_token")

                if not bot_token or not dify_app_config or not isinstance(dify_app_config, dict) or not dify_app_config.get("app_id"):
                    error_message = "Bot Token or Dify App configuration is missing or invalid."
                    logger.error(error_message)
                    if bot_token and channel_id:
                         try:
                             client = WebClient(token=bot_token)
                             client.chat_postMessage(channel=channel_id, text=error_message, thread_ts=thread_ts if channel_type != "im" else None)
                         except Exception as slack_e:
                             logger.error(f"Could not notify user about config error: {slack_e}", exc_info=True)
                    return Response(status=500, response=error_message)

                dify_app_id = dify_app_config["app_id"]
                logger.info(f"Invoking Dify app {dify_app_id} with query: '{query_text}' (Memory disabled)")
                user_id_for_dify = f"slack-user-{user_id}"
                logger.debug(f"(User for logging: {user_id_for_dify})")

                try:
                    response_from_dify = self.session.app.chat.invoke(
                        app_id=dify_app_id,
                        query=query_text,
                        inputs={},
                        response_mode="blocking"
                    )

                    logger.debug(f"Raw response from Dify: {response_from_dify}")
                    dify_answer = response_from_dify.get("answer", "(No response received)")
                    logger.info(f"Received answer from Dify: {dify_answer[:200]}...")

                    try:
                        client = WebClient(token=bot_token)
                        slack_mrkdwn_text = convert_markdown_to_slack(dify_answer)
                        logger.debug(f"Converted Dify answer to Slack mrkdwn (first 200 chars): {slack_mrkdwn_text[:200]}...")
                        result = client.chat_postMessage(
                            channel=channel_id,
                            text=slack_mrkdwn_text,
                            thread_ts=thread_ts if channel_type != "im" else None,
                            mrkdwn=True
                        )
                        logger.info(f"Posted message to Slack channel {channel_id} with mrkdwn: {result.get('ts')}")
                        return Response(
                            status=200,
                            response=json.dumps({"status": "success", "message_ts": result.get('ts')}, ensure_ascii=False),
                            content_type="application/json"
                        )
                    except SlackApiError as e:
                        error_detail = f"Slack API Error posting message: {e.response['error']}"
                        logger.error(error_detail, exc_info=True)
                        return Response(status=500, response=json.dumps({"status": "error", "message": error_detail}), content_type="application/json")

                except Exception as e:
                    error_trace = traceback.format_exc()
                    logger.error(f"Error invoking Dify app or processing response:\n{error_trace}")
                    try:
                        client = WebClient(token=bot_token)
                        error_message_to_user = f"Sorry, an error occurred while processing your request with Dify. Please check logs."
                        client.chat_postMessage(channel=channel_id, text=error_message_to_user, thread_ts=thread_ts if channel_type != "im" else None)
                    except Exception as slack_e:
                        logger.error(f"Could not notify user about Dify invocation error: {slack_e}", exc_info=True)
                    return Response(
                        status=500,
                        response=json.dumps({"status": "error", "message": "Internal Server Error during Dify invocation", "detail": str(e)}),
                        content_type="application/json",
                    )
            else:
                logger.info("No valid query extracted or event type ignored, skipping Dify invocation.")
                return Response(status=200, response="ok, no action needed")

        else:
            logger.info(f"Ignoring top-level request type: {request_type}")
            return Response(status=200, response="ok, ignored top-level type")