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
                        slack_mrkdwn_text = dify_answer

                        def protect_bold(match):
                            return f"%%BOLD_START%%{match.group(1)}%%BOLD_END%%"

                        slack_mrkdwn_text = re.sub(
                            r'^\s*#+\s+(.+)',
                            protect_bold,
                            slack_mrkdwn_text,
                            flags=re.MULTILINE
                        )
                        slack_mrkdwn_text = re.sub(r'\*\*(.+?)\*\*', protect_bold, slack_mrkdwn_text)
                        slack_mrkdwn_text = re.sub(r'__(.+?)__', protect_bold, slack_mrkdwn_text)
                        slack_mrkdwn_text = re.sub(
                            r'(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)',
                            r'_\1_',
                            slack_mrkdwn_text
                        )
                        slack_mrkdwn_text = re.sub(r'~~(.+?)~~', r'~\1~', slack_mrkdwn_text)
                        slack_mrkdwn_text = slack_mrkdwn_text.replace("%%BOLD_START%%", "*").replace("%%BOLD_END%%", "*")
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