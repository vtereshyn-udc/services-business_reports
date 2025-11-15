import sys
import json
import secrets
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request, Form, BackgroundTasks, HTTPException, status, Depends
from fastapi.responses import PlainTextResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials

sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.database import db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")

app: FastAPI = FastAPI(
    openapi_prefix=None,
    docs_url=None,
    redoc_url=None
)
security: HTTPBasic = HTTPBasic()


@app.get(
    path="/sms/receive",
    response_class=PlainTextResponse,
    status_code=status.HTTP_200_OK
)
async def init_webhook(request: Request):
    zd_echo: str = request.query_params.get("zd_echo")
    if not zd_echo:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)

    return PlainTextResponse(zd_echo, status_code=status.HTTP_200_OK)

# @utils.exception
# def update_sms(sms_to: str, otp_code: str) -> None:
#     with open(file=config.sms_path, mode="r") as file:
#         sms_file: list = json.load(file)
#
#     found = False
#     for item in sms_file:
#         if item.get("phone") == sms_to:
#             item["otp_code"] = otp_code
#             item["created_at"] = datetime.now().isoformat()
#             found = True
#             break
#
#     if not found:
#         new_item: dict = {
#             "phone": sms_to,
#             "otp_code": otp_code,
#             "created_at": datetime.now().isoformat(),
#         }
#         sms_file.append(new_item)
#
#     with open(file=config.sms_path, mode="w") as file:
#         json.dump(sms_file, file, indent=2)


@app.post(
    path="/sms/receive",
    response_class=Response,
    status_code=status.HTTP_200_OK
)
async def handle_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        form_data: Form = await request.form()
        payload = dict(form_data)
        logger.info(payload)
    except Exception as e:
        logger.error(e)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)

    if payload.get("event") != "SMS":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)

    try:
        result: dict = json.loads(payload['result'])
    except json.decoder.JSONDecodeError:
        result = None

    if result:
        # sms_from: str = result.get("caller_id")
        sms_to: str = result.get("caller_did")
        sms_text: str = result.get("text")
        otp_code: str = sms_text[:6] if sms_text and sms_text[:6].isdigit() else None
        if not otp_code:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST)

        background_tasks.add_task(db.add_sms, sms_to, otp_code)

    return Response(status_code=status.HTTP_200_OK)


def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    username: bool = secrets.compare_digest(credentials.username, config.API_USER)
    password: bool = secrets.compare_digest(credentials.password, config.API_PASS)

    if not (username and password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

@app.get("/sms/get")
def get_sms(_: None = Depends(verify_credentials)):
    return {"message": "success"}



if __name__ == "__main__":
    uvicorn.run(**config.API)
