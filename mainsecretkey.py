from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import Optional, List
import json
import logging
from datetime import datetime
import uvicorn
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Rakbank POC Kafka User Transaction Webhook (Secure V2)",
    description="Receives user transaction data from Confluent Kafka topic via HTTP Sink with SharedKey validation",
    version="2.0.0"
)

# Shared secret (environment variable)
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "")

def check_token(x_webhook_token: str | None):
    if not WEBHOOK_TOKEN:
        logger.error("[SECURITY] WEBHOOK_TOKEN not configured in environment")
        raise HTTPException(status_code=500, detail="Server misconfiguration: WEBHOOK_TOKEN missing")
    if not x_webhook_token or x_webhook_token != WEBHOOK_TOKEN:
        logger.warning("[SECURITY] Unauthorized access attempt with token: %s", x_webhook_token)
        raise HTTPException(status_code=401, detail="Unauthorized")

class UserTransaction(BaseModel):
    authorizer_usrnbr: Optional[int] = None
    creat_usrnbr: Optional[int] = None
    creat_time: Optional[str] = None
    data: Optional[str] = None
    usrname: Optional[str] = None

received_transactions = []

@app.get("/")
async def root():
    return {
        "message": "Rakbank POC Kafka User Transaction Webhook Service (Secure V2)",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "total_received": len(received_transactions),
        "service_info": {
            "source": "Confluent Cloud Flink",
            "topic": "user_trans_hst_avro",
            "purpose": "POC webhook endpoint with SharedKey"
        }
    }

@app.post("/webhook/user-transactions")
async def receive_user_transaction(
    transaction: UserTransaction,
    x_webhook_token: str | None = Header(default=None)
):
    check_token(x_webhook_token)
    try:
        transaction_dict = transaction.dict()
        transaction_dict["received_at"] = datetime.now().isoformat()
        transaction_dict["poc_id"] = len(received_transactions) + 1
        received_transactions.append(transaction_dict)

        logger.info(f"[POC V2] Transaction received - User: {transaction.usrname}, Creator: {transaction.creat_usrnbr}")
        return {
            "status": "success",
            "message": "User transaction received successfully",
            "poc_id": transaction_dict["poc_id"],
            "user": transaction.usrname,
            "received_at": transaction_dict["received_at"]
        }
    except Exception as e:
        logger.error(f"[POC V2 ERROR] Failed to process transaction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing transaction: {str(e)}")

@app.post("/webhook/user-transactions/batch")
async def receive_batch_transactions(
    transactions: List[UserTransaction],
    x_webhook_token: str | None = Header(default=None)
):
    check_token(x_webhook_token)
    try:
        received_count = 0
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        for transaction in transactions:
            transaction_dict = transaction.dict()
            transaction_dict["received_at"] = datetime.now().isoformat()
            transaction_dict["batch_id"] = batch_id
            transaction_dict["poc_id"] = len(received_transactions) + 1
            received_transactions.append(transaction_dict)
            received_count += 1
            logger.info(f"[POC V2 BATCH] Transaction received - User: {transaction.usrname}")
        return {
            "status": "success",
            "message": f"Batch of {received_count} transactions received successfully",
            "batch_id": batch_id,
            "total_received": len(received_transactions)
        }
    except Exception as e:
        logger.error(f"[POC V2 ERROR] Failed to process batch: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing batch: {str(e)}")

@app.get("/webhook/user-transactions")
async def get_received_transactions(x_webhook_token: str | None = Header(default=None)):
    check_token(x_webhook_token)
    return {
        "total_count": len(received_transactions),
        "last_10_transactions": received_transactions[-10:] if received_transactions else [],
        "last_updated": datetime.now().isoformat(),
        "poc_status": "active"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Rakbank POC Kafka Webhook V2",
        "total_transactions": len(received_transactions)
    }

@app.get("/poc/stats")
async def poc_statistics(x_webhook_token: str | None = Header(default=None)):
    check_token(x_webhook_token)
    if not received_transactions:
        return {
            "message": "No transactions received yet",
            "total_count": 0,
            "status": "waiting_for_data"
        }
    users = [t.get("usrname") for t in received_transactions if t.get("usrname")]
    unique_users = list(set(users))
    return {
        "total_transactions": len(received_transactions),
        "unique_users": len(unique_users),
        "user_list": unique_users,
        "first_transaction": received_transactions[0]["received_at"],
        "last_transaction": received_transactions[-1]["received_at"],
        "poc_status": "collecting_data"
    }

if __name__ == "__main__":
    uvicorn.run("mainsecretkey:app", host="0.0.0.0", port=8000, reload=False, log_level="info")
