import json
import traceback
from typing import Mapping
from werkzeug import Request, Response
from dify_plugin import Endpoint
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

class NewSlackBotEndpoint(Endpoint): 
    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        """
        Invokes the endpoint with the given request.
        """
        
        retry_num = r.headers.get("X-Slack-Retry-Num")
        if (not settings.get("allow_retry") and (r.headers.get("X-Slack-Retry-Reason") == "http_timeout" or ((retry_num is not None and int(retry_num) > 0)))):
            print("Ignoring Slack retry request.") 
            return Response(status=200, response="ok")

        
        try:
            data = r.get_json()
            if data is None:
                print("Request body is empty or not valid JSON.")
                return Response(status=400, response="Bad Request: Empty or invalid JSON.")
        except Exception as e:
            print(f"Failed to parse JSON: {e}")
            return Response(status=400, response="Bad Request: Failed to parse JSON.")

        print(f"Received Slack event data: {json.dumps(data, indent=2)}") 

        
        if data.get("type") == "url_verification":
            challenge_code = data.get("challenge")
            print(f"Handling URL verification, challenge code: {challenge_code}") 
            return Response(
                response=json.dumps({"challenge": challenge_code}),
                status=200,
                content_type="application/json"
            )

        
        if data.get("type") == "event_callback":
            event = data.get("event")
            if not event:
                print("No 'event' field in event_callback.")
                return Response(status=200, response="ok")

            event_type = event.get("type")
            print(f"Received event type: {event_type}") 

            if event_type == "app_mention":
                message = event.get("text", "")
                user_id = event.get("user") 
                channel_id = event.get("channel") 
                ts = event.get("ts") 

                print(f"Received app_mention in channel {channel_id} from user {user_id}: {message}")

                if message.startswith("<@"):
                    parts = message.split("> ", 1)
                    if len(parts) > 1:
                        query_text = parts[1].strip()
                    else:
                        query_text = ""
                else:
                    print("app_mention event text does not start with <@ mention.")
                    return Response(status=200, response="ok")

                print(f"Extracted query: {query_text}")

                dify_app_config = settings.get("app")
                bot_token = settings.get("bot_token")

                if not dify_app_config or not bot_token:
                    error_message = "Dify App or Bot Token is not configured in the plugin settings."
                    print(error_message)
                    
                    try:
                       temp_client = WebClient(token=bot_token if bot_token else "") 
                       if bot_token and channel_id:
                           temp_client.chat_postMessage(channel=channel_id, text=error_message, thread_ts=ts) 
                    except Exception as slack_e:
                       print(f"Could not notify user about config error: {slack_e}")
                    return Response(status=500, response=error_message)

                dify_app_id = dify_app_config.get("app_id")
                if not dify_app_id:
                     error_message = "Could not find app_id in the Dify app configuration."
                     print(error_message)
                     return Response(status=500, response=error_message)

                print(f"Invoking Dify app {dify_app_id} with query: {query_text}")

                try:
                    response_from_dify = self.session.app.chat.invoke(
                        app_id=dify_app_id,
                        query=query_text,
                        inputs={},
                        response_mode="blocking", 
                        user=f"slack-{user_id}" 
                    )
                    dify_answer = response_from_dify.get("answer")
                    print(f"Received answer from Dify: {dify_answer}")

                    try:
                        client = WebClient(token=bot_token)
                        result = client.chat_postMessage(
                            channel=channel_id,
                            text=dify_answer,
                            thread_ts=ts # 元のメッセージのスレッドに返信する
                        )
                        print(f"Posted message to Slack: {result.get('ts')}")
                        return Response(
                            status=200,
                            response=json.dumps({"status": "success", "slack_response": result.data}), # Slack APIの応答を返す
                            content_type="application/json"
                        )
                    except SlackApiError as e:
                        error_detail = f"Slack API Error: {e.response['error']}"
                        print(error_detail)
                        return Response(status=500, response=json.dumps({"status": "error", "message": error_detail}), content_type="application/json")

                except Exception as e:
                    error_trace = traceback.format_exc()
                    print(f"Error invoking Dify app or posting to Slack:\n{error_trace}")
                    try:
                        client = WebClient(token=bot_token)
                        error_message_to_user = f"Sorry, an error occurred while processing your request: {e}"
                        client.chat_postMessage(channel=channel_id, text=error_message_to_user, thread_ts=ts)
                    except Exception as slack_e:
                        print(f"Could not notify user about processing error: {slack_e}")

                    return Response(
                        status=500,
                        response=json.dumps({"status": "error", "message": "Internal Server Error", "detail": str(e)}),
                        content_type="application/json",
                    )

            else:
                print(f"Ignoring event type: {event_type}")
                return Response(status=200, response="ok")

        else:
            print(f"Ignoring request type: {data.get('type')}")
            return Response(status=200, response="ok")
