from motor.motor_asyncio import AsyncIOMotorDatabase


async def get_trades(
    db: AsyncIOMotorDatabase,
    pair: str | None,
    exchange: str | None,
    limit: int,
) -> list[dict]:
    query: dict = {}
    if pair:
        query["pair"] = pair
    if exchange:
        query["exchange"] = exchange
    cursor = db["trades_raw"].find(query, {"_id": 0}).sort("timestamp", -1).limit(limit)
    return await cursor.to_list(length=limit)
